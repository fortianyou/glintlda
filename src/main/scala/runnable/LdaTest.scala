package runnable

import breeze.linalg.SparseVector
import com.github.javacliparser._
import com.typesafe.config.ConfigFactory
import glint.Client
import glintlda.{LDAConfig, Solver}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import you.dataserver.DataServerClient


class LdaTest extends Configurable{

  val fileOpt = new StringOption("file", 'f', "the input corpus file", "")
  val alphaOpt = new FloatOption("alpha", 'a', "the prior alpha of LDA", 0.5f, 0f, 1f)
  val betaOpt = new FloatOption("beta", 'b', "the prior beta of LDA", 0.01f, 0f, 1f)
  val topicOpt = new IntOption("topics", 'K', "the number of topics", 100, 1, Int.MaxValue)
  val partOpt = new IntOption("partitions", 'p', "the number of partitions", 10, 1, Int.MaxValue)
  val iterOpt = new IntOption("iteration", 't', "the number of iterations", 100, 1, Int.MaxValue)
  val cutOffOpt = new FloatOption("cutoff", 'c', "the cutoff rate of vocabulary", 0.4f, 0f, 1f)

  def run(): Unit = {
       val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val file = fileOpt.getValue

    val data: RDD[Array[String]] = sc.textFile(file, partOpt.getValue).map{
      line =>
        line.split("\\s+")
    }

    data.cache()
    val vocab_freq = data.flatMap(_.map((_, 1))).reduceByKey(_ + _).collect()

    //倒序排列
    val vocab_freq_sorted = vocab_freq.sortBy( - _._2)
    println(vocab_freq_sorted.take(10).mkString(" "))
    val word2id = vocab_freq_sorted.map(_._1).zipWithIndex.toMap

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
    val sparsePs = new DataServerClient[Int]("bda02")
    // LDA topic model with 100,000 terms and 100 topics
    val ldaConfig = new LDAConfig()
    ldaConfig.setα(alphaOpt.getValue)
    ldaConfig.setβ(betaOpt.getValue)
    ldaConfig.setTopics(topicOpt.getValue)
    ldaConfig.setVocabularyTerms(word2id.size)
    ldaConfig.setPowerlawCutoff((word2id.size * cutOffOpt.getValue).toInt)
    //ldaConfig.setPowerlawCutoff(word2id.size)
    ldaConfig.setPartitions(partOpt.getValue)
    val model = Solver.fitMetropolisHastings(sc, gc, sparsePs, idData, ldaConfig)

  }
}


/**
  * Created by Roger on 17/3/23.
  */
object LdaTest {

  def main(args: Array[String]): Unit = {

    val string = if (args.length > 0) args.mkString(" ") else "LdaTest"
    val task = ClassOption.cliStringToObject(string, classOf[LdaTest], null)
    task.run()
 }

}
