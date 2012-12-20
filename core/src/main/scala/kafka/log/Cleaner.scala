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
import java.nio._
import java.util.concurrent.Semaphore
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
class Cleaner(val logs: Pool[TopicAndPartition, Log], 
              val logDirs: Seq[File], 
              defaultCleanupPolicy: String,
              topicCleanupPolicy: Map[String, String],
              numThreads: Int = 1,
              val bufferSize: Int = 1024*1024,
              val minDirtyMessages: Long = 1000000L,
              val maxCleanerBytesPerSecond: Double = Double.MaxValue,
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
  private val cleaners = (0 until numThreads).map(_ => new CleanerThread())
  
  /* a throttle used to control the I/O of all the cleaner threads */
  private val throttler = new Throttler(desiredRatePerSec = maxCleanerBytesPerSecond, 
                                        checkIntervalMs = 300, 
                                        throttleDown = true, 
                                        time = time)
  
  /* a hook for testing to synchronize on log cleaning completions */
  private val cleaned = new Semaphore(0)
  
  /* the time to sleep if there are no logs with the key dedupe strategy for log retention */
  private val NoLogsToCleanBackOffMs = 5 * 60 * 1000L
  
  /* the time to sleep if there are logs with the key dedupe strategy but they don't have much to clean */
  private val NotDirtyEnoughBackOffMs = 30 *1000L
  
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
    cleaners.foreach(_.shutdown())
    cleaners.foreach(_.join())
  }
  
  /**
   * For testing, a way to know when work has completed
   */
  def awaitCleaned(count: Int): Unit = cleaned.acquire(count)
  
   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    */
  private def grabFilthiestLog(): LogToClean = {
    lock synchronized {
      val lastClean = checkpoints.values.flatMap(_.read()).toMap
      def isDedupe(topic: String) = topicCleanupPolicy.getOrElse(topic, defaultCleanupPolicy).toLowerCase == "dedupe"
      val eligableLogs = logs.filter(l => isDedupe(l._1.topic))
      val cleanableLogs = eligableLogs.map(l => LogToClean(l._1, l._2, lastClean.getOrElse(l._1, 0)))
      if(cleanableLogs.isEmpty) {
        null
      } else {
        val toClean = cleanableLogs.min
        inProgress += toClean.topicPartition
        toClean
      }
    }
  }
  
  /**
   *  Indicate that we are done cleaning the given log and add it back to the available pool.
   */
  private def doneCleaning(topicAndPartition: TopicAndPartition) {
    lock synchronized {
      inProgress -= topicAndPartition
    }
    cleaned.release()
  }
  
  /**
   * Update the appropriate offset checkpoint file to save out the latest cleaner position
   */
  private def saveCleanerOffset(topicAndPartition: TopicAndPartition, dataDir: File, offset: Long) {
    lock synchronized {
      val checkpoint = checkpoints(dataDir)
      val offsets = checkpoint.read() + ((topicAndPartition, offset))
      checkpoint.write(offsets)
    }
  }

  private class CleanerThread extends Thread {
    val id = threadId.getAndIncrement()
    val running = new AtomicBoolean(true)
    val buffer = ByteBuffer.allocate(bufferSize)
    val offsetMap = new SkimpyOffsetMap
    
    setName("kafka-log-cleaner-thread-" + id)
    setDaemon(false)
    
    def shutdown(): Unit = running.set(false)

    override def run() {
      while(true) {
        try {
          val cleanable = grabFilthiestLog()
          if(cleanable == null)
            // there are no cleanable logs, sleep for a good while--it is possible someone will create a new one, but not that likely
            time.sleep(NoLogsToCleanBackOffMs)
          else if(cleanable.dirtyMessages < minDirtyMessages)
            // even our dirtiest log is pretty clean, sleep for a bit and try again
            time.sleep(NotDirtyEnoughBackOffMs)
          else
            clean(cleanable)
        } catch {
          case e: Exception => error("Uncaught exception in log cleaner: ", e)
        }
      }
    }
    
    private def clean(cleanable: LogToClean) {
      val topic = cleanable.topicPartition.topic
      val part = cleanable.topicPartition.partition
      info("Log cleaner %d beginning cleaning of %s-%d.".format(topic, part))
      val start = time.milliseconds
      val log = cleanable.log
      val segments = log.logSegments
      val dataDir = log.dir.getParentFile
      val endOffset = segments.last.baseOffset - 1
      buildOffsetMap(log, cleanable.lastCleanOffset, endOffset, offsetMap)
      for (group <- groupSegmentsForCleaning(segments.dropRight(1), log.maxSegmentSize))
        cleanSegments(log, group, offsetMap)
      saveCleanerOffset(cleanable.topicPartition, dataDir, endOffset)
      doneCleaning(cleanable.topicPartition)
      val ellapsed = time.milliseconds - start
      info("Log cleaner %d completed cleaning of % in %d ms.".format(id, topic, part))
    }
    
    /* Group log segments into groups of size < max segment size */
    private def groupSegmentsForCleaning(segments: Iterable[LogSegment], maxSize: Int): Seq[Seq[LogSegment]] = {
      var grouped = mutable.ArrayBuffer[Seq[LogSegment]]()
      var size = 0L
      var curr = mutable.ArrayBuffer[LogSegment]()
      for (seg <- segments) {
        if (size + seg.size < maxSize) {
          curr += seg
          size += seg.size
        } else {
          grouped += curr.toSeq
          size = 0
          curr = mutable.ArrayBuffer(seg)
        }
      }
      grouped
    }

    private def cleanSegments(log: Log, segments: Seq[LogSegment], map: SkimpyOffsetMap) {
      // create a new segment with the suffix .cleaned appended to both the log and index name
      val messages = new FileMessageSet(new File(segments.head.log.file.getPath + ".cleaned"))
      val index = new OffsetIndex(new File(segments.head.index.file.getPath + ".cleaned"), segments.head.baseOffset, segments.head.index.maxIndexSize)
      val cleaned = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, SystemTime)

      // clean segments into the new desitnation segment
      for (old <- segments)
        cleanInto(old, cleaned, map)

      // swap in new segment
      swap(log, cleaned, segments)
    }
    
    private def buildOffsetMap(log: Log, start: Long, end: Long, map: SkimpyOffsetMap) {
      map.clear()
      for(segment <- log.logSegments(start, end)) {
        buildOffsetMap(segment, start, end, map)
      }
    }
    
    private def buildOffsetMap(segment: LogSegment, start: Long, end: Long, map: SkimpyOffsetMap) {
      var mapping = segment.translateOffset(start)
      var position = if(mapping == null) 0 else mapping.position
      while(position < segment.log.size) {
        val messages = new ByteBufferMessageSet(segment.log.readInto(buffer, position))
        throttler.maybeThrottle(messages.sizeInBytes)
        for(entry <- messages) {
          if(entry.offset < end)
            return
          val message = entry.message
          require(message.hasKey)
          position += MessageSet.entrySize(message)
          map.put(message.key, entry.offset)
        }
      }
    }

    /**
     * TODO: Optimize this method by doing batch writes of more than one message at a time
     * TODO: Implement proper compression support
     */
    private def cleanInto(source: LogSegment, dest: LogSegment, map: SkimpyOffsetMap) {
      var position = 0
      while (position < source.log.sizeInBytes) {
        val messages = new ByteBufferMessageSet(source.log.readInto(buffer, position))
        throttler.maybeThrottle(messages.sizeInBytes)
        for (entry <- messages) {
          val size = MessageSet.entrySize(entry.message)
          position += size
          dest.append(entry.offset, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, offsetCounter = new AtomicLong(entry.offset), messages = Array(entry.message): _*))
          throttler.maybeThrottle(size)
        }
      }
    }

    /**
     * Add in the new segment and remove all the old segments
     */
    private def swap(log: Log, segment: LogSegment, oldSegments: Seq[LogSegment]) {
      info("Swapping in cleaned segment %d for {%s} in log %s.".format(segment.baseOffset, oldSegments.map(_.baseOffset).mkString(", "), log.name))
      // since we add the new segment first, and since the data is equivalent, we don't need a lock here
      log.addSegment(segment)
      for (s <- oldSegments.tail)
        log.removeSegment(s)
    }
  }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
private case class LogToClean(topicPartition: TopicAndPartition, log: Log, lastCleanOffset: Long) extends Ordered[LogToClean] {
  val dirtyMessages = log.logSegments.last.baseOffset - lastCleanOffset
  override def compare(that: LogToClean): Int = (this.dirtyMessages - that.dirtyMessages).signum
}