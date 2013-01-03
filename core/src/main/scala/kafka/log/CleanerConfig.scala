package kafka.log

/**
 * @param numThreads The number of cleaner threads to run
 * @param bufferSize The size of the buffer used for deduplication, in bytes.
 * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
 * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
 * @param enableClenaer Allows completely disabling the log cleaner
 */
case class CleanerConfig(val numThreads: Int = 1, 
                         val bufferSize: Int = 100*1024*1024,
                         val dedupeBufferLoadFactor: Double = 0.75,
                         val maxIoBytesPerSecond: Double = Double.MaxValue,
                         val backOffMs: Long = 60 * 1000,
                         val enableCleaner: Boolean = true) {
  require(bufferSize / numThreads > 2*1024*1024, "Must have at least 2MB of buffer space per thread.")
}