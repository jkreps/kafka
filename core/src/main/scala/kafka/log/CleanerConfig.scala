package kafka.log

case class CleanerConfig(val numThreads: Int = 1, 
                         val bufferSize: Int = 1024*1024, 
                         val maxIoBytesPerSecond: Double = Double.MaxValue,
                         val minDirtyMessages: Long = 1000000L,
                         val enableCleaner: Boolean = true)