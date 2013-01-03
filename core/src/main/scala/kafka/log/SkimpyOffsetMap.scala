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

import java.util.Arrays
import java.util.concurrent._
import java.security.MessageDigest
import java.nio.ByteBuffer
import kafka.utils.Utils

/**
 * An approximate map used for deduplicating the log
 * @param memory The amount of memory this map can use
 * @param maxLoadFactor The maximum percent full this offset map can be
 */
class SkimpyOffsetMap(val memory: Int, val maxLoadFactor: Double) {
  val map = new ConcurrentHashMap[ByteBuffer, Long]
  
  val bytesPerEntry = 24
  
  /**
   * The maximum number of entries this map can contain
   */
  val capacity: Int = (maxLoadFactor * memory / bytesPerEntry).toInt
  
  def put(key: ByteBuffer, offset: Long): Unit = map.put(key, offset)
  
  def get(key: ByteBuffer): Long = map.get(key)
  
  def clear() = map.clear()
  
  case class SkimpyMapKey(val bytes: Array[Byte]) {
    override def hashCode: Int = Arrays.hashCode(bytes)
    override def equals(t: Any): Boolean = {
      t match {
        case null => false
        case key: SkimpyMapKey => Arrays.equals(bytes, key.bytes)
        case _ => false
      }
    }
  }
  
}