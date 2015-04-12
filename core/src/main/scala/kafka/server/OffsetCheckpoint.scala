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
package kafka.server

import scala.collection._
import scala.collection.JavaConversions._
import kafka.utils.Logging
import kafka.common._
import org.apache.kafka.common._
import java.io._

/**
 * Scalafying wrapper
 */
class OffsetCheckpoint(val file: File) extends Logging {
  private val checkpoint = new org.apache.kafka.common.utils.OffsetCheckpoint(file)

  def write(offsets: Map[TopicAndPartition, Long]) {
    val translated = offsets.map{ case (part: TopicAndPartition, offset: Long) => (new TopicPartition(part.topic, part.partition), offset: java.lang.Long)}
    checkpoint.write(translated)
  }

  def read(): Map[TopicAndPartition, Long] = {
    checkpoint.read().map{ case (part: TopicPartition, offset: java.lang.Long) => (new TopicAndPartition(part.topic, part.partition), offset: Long) }
  }
  
}