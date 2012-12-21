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
  val segmentSize = 100
  val deleteDelay = 1000
  val logDir = TestUtils.tempDir()
  val log1 = TopicAndPartition("log", 1)
  val log2 = TopicAndPartition("log", 2)
  
  @Test
  def testSimpleCleaning() {
    val cleaner = makeCleaner(parts = 2)
    val log = cleaner.logs.get(log1)
    // add messages
    for(i <- 0 until 3; j <- 0 until 100) {
      log.append(TestUtils.singleMessageSet(j.toString.getBytes), assignOffsets = true)
      log.append(TestUtils.singleMessageSet(j.toString.getBytes), assignOffsets = true) 
    }
    cleaner.startup()
    cleaner.awaitCleaned(1)

    // need to validate
    for(i <- 0 until 3; j <- 0 until 100) {
      log.append(TestUtils.singleMessageSet(j.toString.getBytes), assignOffsets = true)
      log.append(TestUtils.singleMessageSet(j.toString.getBytes), assignOffsets = true) 
    }

    // need to validate
  }
    
  @After
  def teardown() {
    Utils.rm(logDir)
  }
  
  
  /* create a cleaner instance and logs with the given parameters */
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
                        scheduler = time.scheduler,
                        maxSegmentSize = segmentSize,
                        maxIndexSize = 1024,
                        segmentDeleteDelayMs = deleteDelay,
                        time = time)
      logs.put(TopicAndPartition("log", i), log)      
    }
  
    new Cleaner(logs = logs,
                logDirs = Seq(logDir),
                defaultCleanupPolicy = defaultPolicy,
                topicCleanupPolicy = policyOverrides,
                numThreads = numThreads,
                minDirtyMessages = minDirtyMessages,
                time = time)
  }

}