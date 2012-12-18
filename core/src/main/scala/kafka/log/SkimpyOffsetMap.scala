package kafka.log

import java.util.Arrays
import java.util.concurrent._
import java.security.MessageDigest
import kafka.utils.Utils

class SkimpyOffsetMap(size: Int) {
  val map = new ConcurrentHashMap[SkimpyMapKey, Long]
  
  def put(key: Array[Byte], offset: Long): Unit = map.put(new SkimpyMapKey(key), offset)
  
  def get(key: Array[Byte]): Long = map.get(new SkimpyMapKey(key))
  
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