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

import java.io.File
import scala.collection._
import org.junit._
import kafka.common.TopicAndPartition
import kafka.utils._
import kafka.message._
import org.scalatest.junit.JUnitSuite

/**
 * Tests for the log cleaner
 */
class CleanerTest extends JUnitSuite {
  
  val time = new MockTime()
  val segmentSize = 10000
  val deleteDelay = 1000
  val logName = "log"
  val logDir = TestUtils.tempDir()
  var counter = 0
  val topics = Array(TopicAndPartition("log", 0), TopicAndPartition("log", 1), TopicAndPartition("log", 2))
  
  def cleanerTest(minDirtyMessages: Int) {
    val cleaner = makeCleaner(parts = 3)
    val log = cleaner.logs.get(topics(0))

    val appends = writeDups(numKeys = 100, numDups = 3, log)
    cleaner.startup()
    
    val lastCleanable = log.logSegments.last.baseOffset - minDirtyMessages
    // wait until we clean up to base_offset of active segment - minDirtyMessages
    cleaner.awaitCleaned("log", 1, 1L)

    // two checks -- (1) total set of key/value pairs should match hash map
    //               (2) the tail of the log should contain no (few) duplicates
    // 

    // need to validate
    cleaner.shutdown()
  }
  
  def writeDups(numKeys: Int, numDups: Int, log: Log): Seq[(Int, Int)] = {
    for(dup <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      counter += 1
      val appendInfo = log.append(TestUtils.singleMessageSet(payload = counter.toString.getBytes, key = key.toString.getBytes), assignOffsets = true)
      (key, count)
    }
  }
    
  @After
  def teardown() {
    Utils.rm(logDir)
  }
  
  /* create a cleaner instance and logs with the given parameters 
   * TODO: should take CleanerConfig instance
   */
  def makeCleaner(parts: Int, 
                  minDirtyMessages: Int = 0, 
                  numThreads: Int = 1,
                  defaultPolicy: String = "dedupe",
                  policyOverrides: Map[String, String] = Map()): Cleaner = {
    
    // create partitions and add them to the pool
    val logs = new Pool[TopicAndPartition, Log]()
    for(i <- 0 until parts) {
      val dir = new File(logDir, "log-1")
      dir.mkdirs()
      val log = new Log(dir = dir,
                        LogConfig(segmentSize = segmentSize, maxIndexSize = 100*1024, fileDeleteDelayMs = deleteDelay),
                        needsRecovery = false,
                        scheduler = time.scheduler,
                        time = time)
      logs.put(TopicAndPartition("log", i), log)      
    }
  
    new Cleaner(CleanerConfig(numThreads = numThreads, minDirtyMessages = minDirtyMessages),
                logDirs = Array(logDir),
                logs = logs,
                time = time)
  }

}