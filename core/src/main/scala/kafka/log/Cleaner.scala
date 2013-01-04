/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import scala.collection._
import scala.math
import java.nio._
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic._
import java.io.File
import kafka.common._
import kafka.message._
import kafka.server.OffsetCheckpoint
import kafka.utils._

/**
 * The cleaner is responsible for removing obsolete records from the log. A message with key K and offset O is obsolete
 * if there exists a message with key K and offset O' such that O < O'. 
 * 
 * This cleaning is carried out by a pool of background threads. Each thread chooses the log with the most new messages and cleans that.
 * 
 * @param logs The logs that should be cleaned
 * @param logDirs The directories where offset checkpoints reside
 * @param threads The number of background threads to use
 * @param bufferSize The size of the buffer to use for reads and writes per thread
 * @param minDirtyMessages The cleaner will not clean any log that doesn't have at least this many new messages since the last cleaning
 * @param maxCleanerBytesPerSecond The maximum bytes/second of I/O (read or write) that the cleaner can process
 * @param time A way to control the passage of time
 */
class Cleaner(val config: CleanerConfig,
              val logDirs: Array[File],
              val logs: Pool[TopicAndPartition, Log], 
              time: Time = SystemTime) extends Logging {
  
  /* the offset checkpoints holding the last cleaned point for each log */
  private val checkpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, "cleaner-offset-checkpoint")))).toMap
  
  /* the set of logs currently being cleaned */
  private val inProgress = mutable.HashSet[TopicAndPartition]()
  
  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  private val lock = new Object
    
  /* a counter for creating unique thread names*/
  private val threadId = new AtomicInteger(0)
  
  /* the threads */
  private val cleaners = (0 until config.numThreads).map(_ => new CleanerThread(config.bufferSize / config.numThreads))
  
  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond, 
                                        checkIntervalMs = 300, 
                                        throttleDown = true, 
                                        time = time)
  
  /* a hook for testing to synchronize on log cleaning completions */
  private val cleaned = new Semaphore(0)
  
  /**
   * Start the background cleaning
   */
  def startup() {
    info("Starting the log cleaner")
    cleaners.foreach(_.start())
  }
  
  /**
   * Stop the background cleaning
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.interrupt())
    cleaners.foreach(_.join())
  }
  
  /**
   * For testing, a way to know when work has completed. This method blocks until the 
   * cleaner has processed up to the given offset on the specified topic/partition
   */
  def awaitCleaned(topic: String, part: Int, offset: Long, timeout: Long = 30000L): Unit = {
    while(!allCleanerCheckpoints.contains(TopicAndPartition(topic, part)))
      cleaned.tryAcquire(timeout, TimeUnit.MILLISECONDS)
  }
  
  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints(): Map[TopicAndPartition, Long] = 
    checkpoints.values.flatMap(_.read()).toMap
  
   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    */
  private def grabFilthiestLog(): Option[LogToClean] = {
    lock synchronized {
      val lastClean = allCleanerCheckpoints()
      // create a LogToClean instance for each log that is eligible for cleaning
      
      val cleanableLogs = logs.filter(l => l._2.config.dedupe)
                              .map(l => LogToClean(l._1, l._2, lastClean.getOrElse(l._1, 0)))  
      val dirtyLogs = cleanableLogs.filter(l => l.totalBytes > 0)
                                   .filter(l => l.cleanableRatio > l.log.config.minCleanableRatio)
                              
      if(dirtyLogs.isEmpty) {
        None
      } else {
        val filthiest = dirtyLogs.max
        inProgress += filthiest.topicPartition
        Some(filthiest)
      }
    }
  }
  
  /**
   *  Indicate that we are done cleaning the given log and add it back to the available pool.
   */
  private def doneCleaning(topicAndPartition: TopicAndPartition, dataDir: File, endOffset: Long) {
    lock synchronized {
      val checkpoint = checkpoints(dataDir)
      val offsets = checkpoint.read() + ((topicAndPartition, endOffset))
      checkpoint.write(offsets)
      inProgress -= topicAndPartition
    }
    cleaned.release()
  }

  private class CleanerThread(memorySize: Int) extends Thread {
    val id = threadId.getAndIncrement()
    val buffer = ByteBuffer.allocate(1024*1024) // buffer for disk I/O
    val offsetMap = new SkimpyOffsetMap(memorySize - buffer.limit, config.dedupeBufferLoadFactor)
    
    setName("kafka-log-cleaner-thread-" + id)
    setDaemon(false)

    override def run() {
      info("Beginning cleaning...")
      try {
        while(!isInterrupted) {
          cleanOrSleep()
        }
      } catch {
        case e: InterruptedException => // all done
      }
      info("Shutting down cleaner.")
    }
    
    private def cleanOrSleep() {
      try {
        grabFilthiestLog() match {
          case None =>
            // there are no cleanable logs, sleep a while
            time.sleep(config.backOffMs)
          case Some(cleanable) =>
            // there's a log, clean it
            clean(cleanable)
        }
      } catch {
        case e: OptimisticLockFailureException => 
          info("Cleaning of log was aborted due to colliding truncate operation.")
      }
    }
    
    private def clean(cleanable: LogToClean) {
      val topic = cleanable.topicPartition.topic
      val part = cleanable.topicPartition.partition
      info("Log cleaner %d beginning cleaning of %s-%d.".format(id, topic, part))
      val start = time.milliseconds
      val log = cleanable.log
      val truncateCount = log.numberOfTruncates
      val dataDir = log.dir.getParentFile
      val upperBoundOffset = math.min(log.activeSegment.baseOffset, cleanable.firstDirtyOffset + offsetMap.capacity)
      val endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1
      for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
        cleanSegments(log, group, offsetMap, truncateCount)
      doneCleaning(cleanable.topicPartition, dataDir, endOffset)
      val ellapsed = time.milliseconds - start
      info("Log cleaner %d completed cleaning of %s-%d in %d ms.".format(id, topic, part, ellapsed))
    }
    
    /* Group log segments into groups of size < max segment size */
    private def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int): List[Seq[LogSegment]] = {
      var grouped = List[List[LogSegment]]()
      var segs = segments.toList
      while(!segs.isEmpty) {
        var group = List(segs.head)
        var logSize = segs.head.size
        var indexSize = 8 * segs.head.index.entries
        segs = segs.tail
        while(!segs.isEmpty && 
              logSize + segs.head.size < maxSize && 
              indexSize + 8 * segs.head.index.entries < maxIndexSize) {
          group = segs.head :: group
          logSize += segs.head.size
          indexSize += 8 * segs.head.index.entries
          segs = segs.tail
        }
        grouped ::= group
      }
      grouped
    }

    private def cleanSegments(log: Log, segments: Seq[LogSegment], map: SkimpyOffsetMap, expectedTruncateCount: Int) {
      // create a new segment with the suffix .cleaned appended to both the log and index name
      val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix)
      logFile.delete()
      val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix)
      indexFile.delete()
      val messages = new FileMessageSet(logFile)
      val index = new OffsetIndex(indexFile, segments.head.baseOffset, segments.head.index.maxIndexSize)
      val cleaned = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, SystemTime)

      // clean segments into the new desitnation segment
      for (old <- segments)
        cleanInto(old, cleaned, map)

      // swap in new segment  
      info("Swapping in cleaned segment %d for {%s} in log %s.".format(cleaned.baseOffset, segments.map(_.baseOffset).mkString(", "), log.name))
      log.swapSegments(cleaned, segments, expectedTruncateCount)
    }
    
    private def buildOffsetMap(log: Log, start: Long, end: Long, map: SkimpyOffsetMap): Long = {
      map.clear()
      val segments = log.logSegments(start, end)
      info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, segments.size, start, end))
      var offset = segments.head.baseOffset
      require(offset == start)
      for(segment <- segments) {
        checkDone()
        offset = buildOffsetMap(segment, map)
      }
      offset
    }
    
    private def buildOffsetMap(segment: LogSegment, map: SkimpyOffsetMap): Long = {
      var position = 0
      var offset = segment.baseOffset
      while(position < segment.log.size) {
        checkDone()
        val messages = new ByteBufferMessageSet(segment.log.readInto(buffer, position))
        throttler.maybeThrottle(messages.sizeInBytes)
        for(entry <- messages) {
          val message = entry.message
          require(message.hasKey)
          position += MessageSet.entrySize(message)
          map.put(message.key, entry.offset)
          offset = entry.offset
        }
      }
      offset
    }

    /**
     * TODO: Optimize this method by doing batch writes of more than one message at a time
     * TODO: Implement proper compression support
     */
    private def cleanInto(source: LogSegment, dest: LogSegment, map: SkimpyOffsetMap) {
      var position = 0
      while (position < source.log.sizeInBytes) {
        checkDone()
        buffer.clear()
        val messages = new ByteBufferMessageSet(source.log.readInto(buffer, position))
        throttler.maybeThrottle(messages.sizeInBytes)
        for (entry <- messages) {
          val size = MessageSet.entrySize(entry.message)
          position += size
          val key = entry.message.key
          if(key == null)
            throw new IllegalStateException("Found null key in log segment %s which is marked as dedupe.".format(source.log.file.getAbsolutePath))
          val lastOffset = map.get(key)
          /* retain the record if it isn't present in the map OR it is present but this offset is the highest (and its not a delete) */
          val retainRecord = lastOffset < 0 || (entry.offset >= lastOffset && entry.message.payload != null)
          if(retainRecord) {
            dest.append(entry.offset, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, offsetCounter = new AtomicLong(entry.offset), messages = Array(entry.message): _*))
            throttler.maybeThrottle(size)
          }
        }
      }
    }
    
    /**
     * If we aren't running any more throw an AllDoneException
     */
    private def checkDone() {
      if(isInterrupted)
        throw new InterruptedException
    }
  }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
private case class LogToClean(topicPartition: TopicAndPartition, log: Log, firstDirtyOffset: Long) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset-1).map(_.size).sum
  val dirtyBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, log.activeSegment.baseOffset)).map(_.size).sum
  val cleanableRatio = dirtyBytes / totalBytes.toDouble
  def totalBytes = cleanBytes + dirtyBytes
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}