package analysis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by Roger on 17/3/23.
  */
object NofreqWordVsDoc {


  def main(args: Array[String]): Unit = {

    //val file = "data/economy_sent_docs_2016_mini"
    val file = "/Users/Roger/workstage/LDA/onlineldavb/economy_sent_docs_2016_mini"
    var flag = true
    val docs = Source.fromFile(file).getLines().map{
      line =>
        if (flag) {
          println(line)
          flag = false
        }
        line.split("\\s+")
    }.toArray

    val word_freq = docs.flatMap(_.toSeq).groupBy(x=>x)
      .map{
        entry =>
          val word = entry._1
          val cnt = entry._2.size
          (word, cnt)
      }

    val unfreq_words = word_freq.filter( x => x._2 < 5 && x._2 > 2).map(_._1).toSet

    val stats = new ArrayBuffer[(Int, Int)]()
    var unFreqWordCnt = 0
    var unFreqDocCnt = 0
    val unFreqHitSet = new mutable.HashSet[String]()

    docs.foreach{
      tokens =>
        tokens.foreach{
          word =>
            if (unfreq_words.contains(word)) {
              unFreqHitSet.add(word)
            }
        }
        if (unFreqWordCnt == 0) {
          println(tokens.mkString(" "))
          println(unFreqHitSet)
        }

        if (unFreqWordCnt < unFreqHitSet.size) {
          unFreqWordCnt = unFreqHitSet.size
          unFreqDocCnt += 1
        }

        stats += ((unFreqWordCnt, unFreqDocCnt))
    }

    println( "x = np.array([" + stats.map(_._1).mkString(", ") + "])")
    println( "y = np.array([" + stats.map(_._2).mkString(", ") + "])")


  }
}
