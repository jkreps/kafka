package kafka.log

import scala.collection._
import java.nio._
import java.util.concurrent.atomic._
import java.io.File
import kafka.message._
import kafka.server.OffsetCheckpoint
import kafka.utils._

class Cleaner(val logDirs: Seq[File]) {
  logDirs.map(dir => (dir, new OffsetCheckpoint(dir))).toMap
  
  
  def startup() {
    
  }
  
  def shutdown() {
    
  }

}

class CleanerThread(val logs: mutable.Buffer[Log], val lock: Object, val bufferSize: Int) extends Runnable {
  
  val buffer = ByteBuffer.allocate(bufferSize)
  
  def run() {
    val log = filthiestLog()
    // build map from last checkpoint to last segment
    val grouped = groupSegmentsForCleaning(log.logSegments, 1024*1024*1024) /* TODO: make size configurable */
    for(group <- grouped)
      clean(log, group, null)
    // checkpoint cleaner point
  }
  
  /* Group log segments into groups of size < max segment size */
  def groupSegmentsForCleaning(segments: Iterable[LogSegment], maxSize: Int): Seq[Seq[LogSegment]] = {
    var grouped = mutable.ArrayBuffer[Seq[LogSegment]]()
    var size = 0L
    var curr = mutable.ArrayBuffer[LogSegment]()
    for(seg <- segments) {
      if(size + seg.size < maxSize) {
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
  
  def clean(log: Log, segments: Seq[LogSegment], map: SkimpyOffsetMap) {
    // create a new segment with the suffix .cleaned appended to both the log and index name
    val messages = new FileMessageSet(new File(segments.head.log.file.getPath + ".cleaned"))
    val index = new OffsetIndex(new File(segments.head.index.file.getPath + ".cleaned"), segments.head.baseOffset, segments.head.index.maxIndexSize)
    val segment = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, SystemTime)
    
    // clean segments
    for(old <- segments)
      cleanTo(old, segment, map)
      
    // swap in new segment
    swapIn(log, segment, segments)
  }
  
  def cleanTo(source: LogSegment, dest: LogSegment, map: SkimpyOffsetMap) {
    var position = 0
    while(position < source.log.sizeInBytes) {
      val messages = new ByteBufferMessageSet(source.log.readInto(buffer, position))
      // check messages until we find a message we need to eliminate, then write them to the dest
      // need to handle compressed messages
      for(entry <- messages) {
        position += MessageSet.entrySize(entry.message)
        /* TODO: write larger sets, optimize compression */
        dest.append(entry.offset, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, offsetCounter = new AtomicLong(entry.offset), messages = Array(entry.message):_*))
      }
    }
  }
  
  /**
   * Add in the new segment and remove all the old segments
   */
  def swapIn(log: Log, segment: LogSegment, oldSegments: Seq[LogSegment]) {
    // since we add the new segment first, and since the data is equivalent, we don't need a lock here
    log.addSegment(segment)
    for(s <- oldSegments.tail)
      log.removeSegment(s)
    // todo: delete actual segments
  }
  
  def filthiestLog(): Log = {
    null
  }
}