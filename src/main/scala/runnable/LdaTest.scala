package runnable

import breeze.linalg.SparseVector
import com.typesafe.config.ConfigFactory
import glint.Client
import glintlda.{LDAConfig, Solver}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roger on 17/3/23.
  */
object LdaTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val file = args(0)

    val data: RDD[Array[String]] = sc.textFile(file, 10).map{
      line =>
        line.split("\\s+")
    }

    data.cache()
    val vocab_freq = data.flatMap(_.map((_, 1))).reduceByKey(_ + _).collect()

    //倒序排列
    val vocab_freq_sorted = vocab_freq.sortBy( - _._2).map(_._1)
    val word2id = vocab_freq_sorted.zipWithIndex.toMap

    val idData = data.map{
      words =>
        val doc = SparseVector.zeros[Int](word2id.size)
        words.foreach{
          word =>
            val wid = word2id(word)
            doc(wid) += 1
        }
        doc
    }

    val gc = Client(ConfigFactory.parseFile(new java.io.File("resources/glint.conf")))

    // LDA topic model with 100,000 terms and 100 topics
    val ldaConfig = new LDAConfig()
    ldaConfig.setα(0.5)
    ldaConfig.setβ(0.01)
    ldaConfig.setTopics(100)
    ldaConfig.setVocabularyTerms(word2id.size)
    ldaConfig.setPowerlawCutoff(word2id.size * 2 / 5)
    ldaConfig.setPartitions(10)
    val model = Solver.fitMetropolisHastings(sc, gc, idData, ldaConfig)
  }

}
