package glintlda

import breeze.linalg.{DenseVector, SparseVector, sum}
import glintlda.util.FastRNG

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A Gibbs sample
  * @param features The (sequential and ordered) features
  * @param topics The assigned topics
  */
class GibbsSample(val features: Array[Int],
                  val topics: Array[Int],
                  val docId: Int,
                  val hasUnFreq: Boolean) extends Serializable {

  /**
    * Returns a dense topic count
    * @param nrOfTopics The total number of topics
    * @return The dense topic count
    */
  def denseCounts(nrOfTopics: Int): DenseVector[Int] = {
    val result = DenseVector.zeros[Int](nrOfTopics)
    var i = 0
    while (i < topics.length) {
      result(topics(i)) += 1
      i += 1
    }
    result
  }

  /**
    * Returns a sparse topic count
    *
    * @param nrOfTopics The total number of topics
    * @return The sparse topic count
    */
  def sparseCounts(nrOfTopics: Int): SparseVector[Int] = {
    val result = SparseVector.zeros[Int](nrOfTopics)
    var i = 0
    while (i < topics.length) {
      result.update(topics(i), result(topics(i)) + 1)
      i += 1
    }
    result
  }

}

class WDReverseGibbsSample(override val features: Array[Int],
                           override val topics: Array[Int],
                           val localtion: Array[Byte])
  extends GibbsSample(features, topics, -1, false){

  def denseWordCounts(nrOfTopics: Int): DenseVector[Int] = {
    val result = DenseVector.zeros[Int](nrOfTopics)
    var i = 0
    while (i < topics.length) {
      result(topics(i)) += 1
      i += 1
    }
    result
  }

  def sparseWordCounts(nrOfTopics: Int): SparseVector[Int] = {
    val result = SparseVector.zeros[Int](nrOfTopics)
    var i = 0
    while (i < topics.length) {
      result.update(topics(i), result(topics(i)) + 1)
      i += 1
    }
    result
  }
}


class FreqAwareGibbsSample(val freqSample: GibbsSample,
                           val unFreqSample: GibbsSample,
                           val docId: Int) extends Serializable{

  /**
    * Returns a dense topic count
    * @param nrOfTopics The total number of topics
    * @return The dense topic count
    */
  def denseCounts(nrOfTopics: Int): DenseVector[Int] = {
    //TODO: the n(d,t) of unFreq words
    freqSample.denseCounts(nrOfTopics)
  }

  def sparseCounts(nrOfTopics: Int): SparseVector[Int] = {
    //TODO: the n(d,t) of unFreq words
    freqSample.sparseCounts(nrOfTopics)
  }

}

object GibbsSample {

  /**
    * Initializes a Gibbs sample with random (uniform) topic assignments
    *
    * @param sv The sparse vector representing the document
    * @param random The random number generator
    * @param topics The number of topics
    * @return An initialized Gibbs sample with random (uniform) topic assignments
    */
  def apply(sv: SparseVector[Int], random: FastRNG, topics: Int, docId: Int): GibbsSample = {
    val totalTokens = sum(sv)
    val sample = new GibbsSample(new Array[Int](totalTokens), new Array[Int](totalTokens), docId, false)

    var i = 0
    var current = 0
    while (i < sv.activeSize) {
      val index = sv.indexAt(i)
      var value = sv.valueAt(i)
      while (value > 0) {
        sample.features(current) = index
        sample.topics(current) = random.nextPositiveInt() % topics
        current += 1
        value -= 1
      }
      i += 1
    }

    sample
  }

  def apply(lowFreqWords: Array[(String, Array[(Int, GibbsSample)])],
            random: FastRNG, topics: Int):
  (Map[String, Byte], mutable.HashMap[Int, WDReverseGibbsSample]) = {
    val location = mutable.HashMap[String, Byte]()
    val len = lowFreqWords.length
    Range(0, len).foreach{
      i =>
      val host = lowFreqWords(i)._1
      if (!location.contains(host)) {
        location(host) = location.size.toByte
      }
    }
    // reverse position of word and doc (word is a collection of doc)
    val reverseGibbsSample = new mutable.HashMap[Int, (ArrayBuffer[Int], ArrayBuffer[Int], ArrayBuffer[Byte])]
    lowFreqWords.foreach {
      case (host, dataset) =>
        val locate = location(host)
        dataset.foreach {
          case (docId, sv) =>

            for (i <- 0 until sv.features.length) {
              val word = sv.features(i)
              if (!reverseGibbsSample.contains(word)) {
                reverseGibbsSample += ((word,
                  (new ArrayBuffer[Int](), new ArrayBuffer[Int](), new ArrayBuffer[Byte])))
              }

              val rtopic = sv.topics(i)
              reverseGibbsSample(word)._1.append(docId) // doc
              reverseGibbsSample(word)._2.append(rtopic)
              reverseGibbsSample(word)._3.append(locate)
            }
        }
    }

    (location.toMap, reverseGibbsSample.map {
      case (word, entry) =>
        (word, new WDReverseGibbsSample(entry._1.toArray, entry._2.toArray, entry._3.toArray))
    }
      )

  }

  /**
    * Initializes a Gibbs sample with random (uniform) topic assignments
    *
    * @param sv The sparse vector representing the document
    * @param random The random number generator
    * @param topics The number of topics
    * @param cutoff The start index of low-frequency words
    * @return An initialized Gibbs sample with random (uniform) topic assignments
    */
  def apply(sv: SparseVector[Int], random: FastRNG, topics: Int, cutoff: Int, docId: Int): FreqAwareGibbsSample = {
    val cutOffIndex = binarySearch(sv, cutoff)
    var freqTotalTokens = 0
    var unFreqTotalTokens = 0
    var i:Int = 0
    while(i < sv.activeSize) {
      if (i < cutOffIndex) freqTotalTokens += sv.valueAt(i)
      else unFreqTotalTokens += sv.valueAt(i)
      i += 1
    }


    val hasUnFreq = unFreqTotalTokens > 0

    val freqSample = new GibbsSample(new Array[Int](freqTotalTokens), new Array[Int](freqTotalTokens), docId, hasUnFreq)
    val unFreqSample = new GibbsSample(new Array[Int](unFreqTotalTokens), new Array[Int](unFreqTotalTokens), docId, hasUnFreq)

    var current = 0
    i = 0
    while (i < cutOffIndex) {
      val index = sv.indexAt(i)
      var value = sv.valueAt(i)
      while (value > 0) {
        freqSample.features(current) = index
        freqSample.topics(current) = random.nextPositiveInt() % topics
        current += 1
        value -= 1
      }
      i += 1
    }

    current = 0
    while( i < sv.activeSize) {
      val index = sv.indexAt(i)
      var value = sv.valueAt(i)
      while (value > 0) {
        while (value > 0) {
          unFreqSample.features(current) = index
          unFreqSample.topics(current) = random.nextPositiveInt() % topics
          current += 1
          value -= 1
        }
        i += 1
      }
    }

    new FreqAwareGibbsSample(freqSample, unFreqSample, docId)
  }

  /**
    *
    * @param sv
    * @param cutOff
    * @return 返回cutOff的lower bound, 代表稀疏矩阵链表的位置
    */
  def binarySearch(sv: SparseVector[Int], cutOff: Int): Int = {
    val len = sv.activeSize
    var start = 0
    var end = len

    while(start < end) {
      val mid = start + ((end - start) >> 1)

      if( sv.indexAt(mid) < cutOff) {
        start = mid + 1
      } else if (sv.indexAt(mid) == cutOff) {
        return mid
      } else {
        end = mid
      }
    }

    return end
  }
}
