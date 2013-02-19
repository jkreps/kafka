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

package kafka

import joptsimple.OptionParser
import java.util.Properties
import java.util.Random
import java.io._
import scala.io.Source
import scala.io.BufferedSource
import kafka.producer._
import kafka.consumer._
import kafka.serializer._
import kafka.utils._

/**
 * This is a torture test that runs against an existing broker. Here is how it works:
 * 
 * It produces a series of specially formatted messages to one or more partitions. Each message it produces
 * it logs out to a text file. The messages have a limited set of keys, so there is duplication in the key space.
 * 
 * The broker will clean its log as the test runs.
 * 
 * When the specified number of messages have been produced we create a consumer and consume all the messages in the topic
 * and write that out to another text file.
 * 
 * Using a stable unix sort we sort both the producer log of what was sent and the consumer log of what was retrieved by the message key. 
 * Then we compare the final message in both logs for each key. If this final message is not the same for all keys we
 * print an error and exit with exit code 1, otherwise we print the size reduction and exit with exit code 0.
 */
object TestLogCleaning {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send or consume.")
                               .withRequiredArg
                               .describedAs("count")
                               .ofType(classOf[java.lang.Long])
                               .defaultsTo(Long.MaxValue)
    val numDupsOpt = parser.accepts("duplicates", "The number of duplicates for each key.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(5)
    val brokerOpt = parser.accepts("broker", "Url to connect to.")
                          .withRequiredArg
                          .describedAs("url")
                          .ofType(classOf[String])
    val topicsOpt = parser.accepts("topics", "The number of topics to test.")
                          .withRequiredArg
                          .describedAs("count")
                          .ofType(classOf[java.lang.Integer])
                          .defaultsTo(1)
    val percentDeletesOpt = parser.accepts("percent-deletes", "The percentage of updates that are deletes.")
    		                      .withRequiredArg
    		                      .describedAs("percent")
    		                      .ofType(classOf[java.lang.Integer])
    		                      .defaultsTo(0)
    val zkConnectOpt = parser.accepts("zk", "Zk url.")
                             .withRequiredArg
                             .describedAs("url")
                             .ofType(classOf[String])
    val sleepSecsOpt = parser.accepts("sleep", "Time to sleep between production and consumption.")
                             .withRequiredArg
                             .describedAs("ms")
                             .ofType(classOf[java.lang.Integer])
                             .defaultsTo(0)
    val cleanupOpt = parser.accepts("cleanup", "Delete temp files when done.")
    
    val options = parser.parse(args:_*)
    
    if(!options.has(brokerOpt) || !options.has(zkConnectOpt) || !options.has(numMessagesOpt)) {
      parser.printHelpOn(System.err)
      System.exit(1)
    }
    
    // parse options
    val messages = options.valueOf(numMessagesOpt).longValue
    val percentDeletes = options.valueOf(percentDeletesOpt).intValue
    val dups = options.valueOf(numDupsOpt).intValue
    val brokerUrl = options.valueOf(brokerOpt)
    val topicCount = options.valueOf(topicsOpt).intValue
    val zkUrl = options.valueOf(zkConnectOpt)
    val sleepSecs = options.valueOf(sleepSecsOpt).intValue
    val cleanup = options.has(cleanupOpt)
    
    val testId = new Random().nextInt(Int.MaxValue)
    val topics = (0 until topicCount).map("log-cleaner-test-" + testId + "-" + _).toArray
    
    println("Producing %d messages...".format(messages))
    val producedDataFile = produceMessages(brokerUrl, topics, messages, dups, percentDeletes, cleanup)
    println("Sleeping for %d seconds...".format(sleepSecs))
    Thread.sleep(sleepSecs * 1000)
    println("Consuming messages...")
    val consumedDataFile = consumeMessages(zkUrl, topics, cleanup)
    
    val producedLines = lineCount(producedDataFile)
    val consumedLines = lineCount(consumedDataFile)
    val reduction = 1.0 - consumedLines.toDouble/producedLines.toDouble
    println("%d rows of data produced, %d rows of data consumed (%.1f%% reduction).".format(producedLines, consumedLines, 100 * reduction))
    
    println("Validating output files...")
    validateOutput(externalSort(producedDataFile), externalSort(consumedDataFile))
  }
  
  def lineCount(file: File): Int = io.Source.fromFile(file).getLines.size
  
  def validateOutput(producedReader: BufferedReader, consumedReader: BufferedReader) {
    val produced = valuesIterator(producedReader)
    val consumed = valuesIterator(consumedReader)
    var total = 0
    var mismatched = 0
    while(produced.hasNext && consumed.hasNext) {
      val p = produced.next()
      val c = consumed.next()
      if(p != c) {
        println("Mismatched values found in iteration: produced = " + p + " consumed = " + c)
        mismatched += 1
      }
      total += 1
    }
    require(!produced.hasNext, "Additional values produced not found in consumer log.")
    require(!consumed.hasNext, "Additional values consumed not found in producer log.")
    println("Validated " + total + " values, " + mismatched + " mismatches.")
    require(mismatched == 0, "Non-zero number of row mismatches.")
  }
  
  def valuesIterator(reader: BufferedReader) = {
    new IteratorTemplate[TestRecord] {
      def makeNext(): TestRecord = {
        var next = readNext(reader)
        while(next != null && next.delete)
          next = readNext(reader)
        if(next == null)
          allDone()
        else
          next
      }
    }
  }
  
  def readNext(reader: BufferedReader): TestRecord = {
    var line = reader.readLine()
    if(line == null)
      return null
    var curr = new TestRecord(line)
    while(true) {
      line = peekLine(reader)
      if(line == null)
        return curr
      val next = new TestRecord(line)
      if(next == null || next.topicAndKey != curr.topicAndKey)
        return curr
      curr = next
      reader.readLine()
    }
    null
  }
  
  def peekLine(reader: BufferedReader) = {
    reader.mark(4096)
    val line = reader.readLine
    reader.reset()
    line
  }
  
  def externalSort(file: File): BufferedReader = {
    val builder = new ProcessBuilder("sort", "--key=1,2", "--stable", "--buffer-size=20%", file.getAbsolutePath)
    val process = builder.start()
    new BufferedReader(new InputStreamReader(process.getInputStream()))
  }
  
  def produceMessages(brokerUrl: String, 
                      topics: Array[String], 
                      messages: Long, 
                      dups: Int,
                      percentDeletes: Int,
                      cleanup: Boolean): File = {
    val producerProps = new Properties
    producerProps.setProperty("producer.type", "async")
    producerProps.setProperty("broker.list", brokerUrl)
    producerProps.setProperty("serializer.class", classOf[StringEncoder].getName)
    producerProps.setProperty("key.serializer.class", classOf[StringEncoder].getName)
    producerProps.setProperty("queue.enqueue.timeout.ms", "-1")
    producerProps.setProperty("batch.size", 1000.toString)
    val producer = new Producer[String, String](new ProducerConfig(producerProps))
    val rand = new Random(1)
    val keyCount = (messages / dups).toInt
    val producedFile = File.createTempFile("kafka-log-cleaner-produced-", ".txt")
    println("Logging produce requests to " + producedFile.getAbsolutePath)
    if(cleanup)
      producedFile.deleteOnExit()
    val producedWriter = new BufferedWriter(new FileWriter(producedFile), 1024*1024)
    for(i <- 0L until (messages * topics.length)) {
      val topic = topics((i % topics.length).toInt)
      val key = rand.nextInt(keyCount)
      val delete = i % 100 < percentDeletes
      val message = 
        if(delete)
          KeyedMessage[String, String](topic = topic, key = key.toString, message = null)
        else
          KeyedMessage[String, String](topic = topic, key = key.toString, message = i.toString)
      producer.send(message)
      producedWriter.write(TestRecord(topic, key, i, delete).toString)
      producedWriter.newLine()
    }
    producedWriter.close()
    producer.close()
    producedFile
  }
  
  def consumeMessages(zkUrl: String, topics: Array[String], cleanup: Boolean): File = {
    val consumerProps = new Properties
    consumerProps.setProperty("group.id", "log-cleaner-test-" + new Random().nextInt(Int.MaxValue))
    consumerProps.setProperty("zk.connect", zkUrl)
    consumerProps.setProperty("consumer.timeout.ms", (5*1000).toString)
    val connector = new ZookeeperConsumerConnector(new ConsumerConfig(consumerProps))
    val streams = connector.createMessageStreams(topics.map(topic => (topic, 1)).toMap, new StringDecoder, new StringDecoder)
    val consumedFile = File.createTempFile("kafka-log-cleaner-consumed-", ".txt")
    println("Logging consumed messages to " + consumedFile.getAbsolutePath)
    if(cleanup)
      consumedFile.deleteOnExit()
    val consumedWriter = new BufferedWriter(new FileWriter(consumedFile))
    for(topic <- topics) {
      val stream = streams(topic).head
      try {
        for(item <- stream) {
          val delete = item.message == null
          val value = if(delete) -1L else item.message.toLong
          consumedWriter.write(TestRecord(topic, item.key.toInt, value, delete).toString)
          consumedWriter.newLine()
        }
      } catch {
        case e: ConsumerTimeoutException => 
      }
    }
    consumedWriter.close()
    connector.shutdown()
    consumedFile
  }
  
}

case class TestRecord(val topic: String, val key: Int, val value: Long, val delete: Boolean) {
  def this(pieces: Array[String]) = this(pieces(0), pieces(1).toInt, pieces(2).toLong, pieces(3) == "d")
  def this(line: String) = this(line.split("\t"))
  override def toString() = topic + "\t" +  key + "\t" + value + "\t" + (if(delete) "d" else "u")
  def topicAndKey = topic + key
}