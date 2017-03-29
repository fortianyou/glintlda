package you.dataserver

import java.util

import redis.clients.jedis._

import scala.{specialized => spec}
import scala.reflect.ClassTag
import scala.util.Random
import collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Roger on 17/3/24.
  */
class DataServerClient[@spec(Double, Int, Long, Float) T]
(val host: String = "localhost",
 val port: Int = 6379,
 val bufferSize: Int = 100000)
(implicit m: ClassTag[T]) extends Serializable{

  @transient
  lazy val jedisPool = new JedisPool(host, port)

  private val bufferDKeys = new Array[String](bufferSize)
  private val bufferTKeys = new Array[String](bufferSize)
  private val bufferValues = new Array[T](bufferSize)
  private var bufferIndex = 0

  private val incBufferDKeys = new Array[String](bufferSize)
  private val incBufferTKeys = new Array[String](bufferSize)
  private val incBufferValues = new Array[Int](bufferSize)
  private var incBufferIndex = 0

  private val delBufferDKeys = new Array[String](bufferSize)
  private val delBufferTKeys = new Array[String](bufferSize)
  private var delBufferIndex = 0

  def incKey(key: String): Int = {
    var jedis: Jedis = null

    try {
      jedis = jedisPool.getResource
      jedis.incr(key).toInt
    } finally {
      if (jedis != null)
      jedis.close()
    }
  }

  def getKey(key: String): Int = {
    var jedis: Jedis = null

    try {
      jedis = jedisPool.getResource
      jedis.get(key).toInt
    } finally {
      if (jedis != null)
      jedis.close()
    }
  }

  def resetKey(key: String): Unit = {
    var jedis: Jedis = null

    try {
      jedis = jedisPool.getResource
      jedis.del(key)
    } finally {
      if (jedis != null)
      jedis.close()
    }
  }

  @inline
  def pushToBuffer(key1: Int, key2: Int, value: T): Boolean = {
    if (isFull) {
      return false
    } else {
      bufferDKeys(bufferIndex) = DataServerClient.keyGen(key1)
      bufferTKeys(bufferIndex) = DataServerClient.keyGen(key2)
      bufferValues(bufferIndex) = value
      bufferIndex += 1
      return true
    }
  }

  def delBufferred(key1: Int, key2: Int): Boolean = {
    if (isDelBuffFull) {
      return false
    } else {
      delBufferDKeys(delBufferIndex) = DataServerClient.keyGen(key1)
      delBufferTKeys(delBufferIndex) = DataServerClient.keyGen(key2)
      delBufferIndex += 1
      return true
    }

  }

  def increaseBufferred(key1: Int, key2: Int, value: Int): Boolean = {
    if (isIncBuffFull) {
      return false
    } else {
      incBufferDKeys(incBufferIndex) = DataServerClient.keyGen(key1)
      incBufferTKeys(incBufferIndex) = DataServerClient.keyGen(key2)
      incBufferValues(incBufferIndex) = value
      incBufferIndex += 1
      return true
    }
  }

  @inline
  def isIncBuffFull = (bufferSize == incBufferIndex)

  @inline
  def isFull = (bufferSize == bufferIndex)

  @inline
  def isDelBuffFull = (bufferSize == delBufferIndex)

  def flushDelBuffer(): Boolean = {
    if (delBufferIndex == 0) {
      return true
    } else {
      var jedis: Jedis = null
      try {
        jedis = jedisPool.getResource
        val pipeline = jedis.pipelined()
        for (i <- 0 until delBufferIndex) {
          pipeline.hdel(delBufferDKeys(i), delBufferTKeys(i))
        }
        pipeline.sync()
        pipeline.close()
        delBufferIndex = 0
        return true
      } catch {
         case x:Exception =>
          x.printStackTrace()
          return false
      } finally {
        if (jedis != null) jedis.close()
      }
    }
  }




  def flushIncBuffer(): Boolean = {
    if (incBufferIndex == 0) {
      true
    } else {
      var jedis: Jedis = null
      try {
        jedis = jedisPool.getResource
        val pipeline = jedis.pipelined()
        for (i <- 0 until incBufferIndex) {
          pipeline.hincrBy(incBufferDKeys(i), incBufferTKeys(i), incBufferValues(i))
        }
        pipeline.sync()
        pipeline.close()
        incBufferIndex = 0
        return true
      } catch {
         case x:Exception =>
          return false
      } finally {
        if (jedis != null) jedis.close
      }
    }
  }

  /** is not safe
  def flush(): Unit = {
    flushBuffer()
    flushIncBuffer()
    flushDelBuffer()
  }*/

  def flushBuffer(): Boolean = {
    if (bufferIndex == 0) {
      true
    } else {
      var jedis: Jedis = null
      try {
        jedis = jedisPool.getResource
        val pipeline = jedis.pipelined()
        for (i <- 0 until bufferIndex) {
          pipeline.hset(bufferDKeys(i), bufferTKeys(i), bufferValues(i).toString)
        }
        pipeline.sync()
        pipeline.close()
        bufferIndex = 0
        return true
      } catch{
        case x:Exception =>
          x.printStackTrace()
          return false
      } finally{
        if (jedis != null) jedis.close()
      }

    }
  }

  @inline
  private def pull(key1: Int, key2: Int, pipeline: Pipeline): Response[String] = {
    val Dkey = DataServerClient.keyGen(key1)
    val Tkey = DataServerClient.keyGen(key2)
    pipeline.hget(Dkey, Tkey)
  }

  def pull(key1s: IndexedSeq[Int], key2s: IndexedSeq[Int]): IndexedSeq[T] = {
    val len = key1s.length
    assert(len == key2s.length)
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      val results: IndexedSeq[Response[String]] = for (i <- 0 until len) yield {
        pull(key1s(i), key2s(i), pipeline)
      }
      pipeline.sync()
      pipeline.close()
      parse(results)
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  @inline
  private def pull(key1: Int, pipeline: Pipeline)
  : Response[util.Map[String, String]] = {
    val Dkey = DataServerClient.keyGen(key1)
    val results: Response[util.Map[String, String]] = pipeline.hgetAll(Dkey)
    return results
  }

  def pull(key1s: IndexedSeq[Int]): mutable.HashMap[Int, mutable.Map[Int, T]] = {
      var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()

      val results = mutable.HashMap[Int, mutable.Map[Int, T]]()
      val responses: IndexedSeq[Response[util.Map[String, String]]] = key1s.map{
        key1 =>
        pull(key1, pipeline)
      }
      pipeline.sync()
      pipeline.close()

      for (i <- 0 until key1s.length) {
          results(key1s(i)) = parse(responses(i))
      }

      results
    } finally{
      if (jedis != null) jedis.close()
    }
  }

  def pull(key1: Int): scala.collection.mutable.Map[Int, T] = {
     var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      val results = pull(key1, pipeline)
      pipeline.sync()
      pipeline.close()
      parse(results)
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  private def parse( results: Response[java.util.Map[String, String]] ):
  scala.collection.mutable.Map[Int, T] = {
    if (results.get() == null) return null
    val clazz = m.runtimeClass
      if (classOf[Int] isAssignableFrom clazz) {
        results.get().map{
          case x=>
            (DataServerClient.keyDecode(x._1), x._2.toInt.asInstanceOf[T])
        }
      }
      else if (classOf[Float] isAssignableFrom clazz) {
        results.get().map{
          case x=>
            (DataServerClient.keyDecode(x._1), x._2.toFloat.asInstanceOf[T])
        }
      }
      else if (classOf[Double] isAssignableFrom clazz) {
        results.get().map{
          case x=>
            (DataServerClient.keyDecode(x._1), x._2.toDouble.asInstanceOf[T])
        }
      }
      else {
        results.get().map{
          case x=>
            (DataServerClient.keyDecode(x._1), x._2.toLong.asInstanceOf[T])
        }
      }
  }

  private def parse(results: IndexedSeq[Response[String]]): IndexedSeq[T] = {
    val clazz = m.runtimeClass
      if (classOf[Int] isAssignableFrom clazz){
        results.map{
          case x =>
            if (x.get == null) 0.asInstanceOf[T]
            else x.get().toInt.asInstanceOf[T]
        }
      }
      else if (classOf[Float] isAssignableFrom clazz){
        results.map{
          case x =>
            if (x.get == null) 0.asInstanceOf[T]
            else x.get().toFloat.asInstanceOf[T]
        }
      }
      else if (classOf[Double] isAssignableFrom clazz){
        results.map{
          case x =>
            if (x.get == null) 0.asInstanceOf[T]
            else x.get().toDouble.asInstanceOf[T]
        }
      }
      else {
        results.map{
          case x =>
            if (x.get == null) 0.asInstanceOf[T]
            else x.get().toLong.asInstanceOf[T]
        }
      }
  }

  def clear(): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      jedis.flushAll()
    } finally {
      if (jedis != null) jedis.close()
    }
  }
  def destroy(): Unit = {
    clear()
    jedisPool.close()
  }
}

object DataServerClient{

  private def keyGen(key: Int): String ={
    val chars = new Array[Char](4)
    val mode = 255
    for (i <- 0 until 4) {
        chars(i) = ((key >> (8 * i)) & mode).toChar
    }
    new String(chars)
  }

  @inline
  private def keyDecode(key: String) = {
    val bytes = key.toCharArray

    var code = 0
    for(k <- 0 until 4){
        code += (bytes(k).toInt << (k * 8))
    }

    code
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 until 1000) {
      val key1 = Random.nextInt()
      val key = keyGen(key1)
    //  println(key)
     // println((key1, key2))
     // println(client.keyDecode(key))
      assert(key1 == keyDecode(key))
    }
  }
}
