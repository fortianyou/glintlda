package glintlda

import java.io._

import akka.util.Timeout
import glint.Client
import glint.iterators.RowBlockIterator
import glint.models.client.buffered.BufferedBigMatrix
import glint.models.client.granular.GranularBigMatrix
import glint.models.client.retry.{RetryBigMatrix, RetryBigVector}
import glint.models.client.{BigMatrix, BigVector}
import glint.partitioning.cyclic.CyclicPartitioner
import glintlda.util.{SimpleLock, BoundedPriorityQueue, ranges}
import org.apache.spark.mllib.clustering.LocalLDAModel

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
  * A distributed LDA Model
  *
  * @param wordTopicCounts A big matrix representing the topic word counts
  * @param topicCounts A big vector representing the global topic counts
  * @param config The LDA configuration
  */
class LDAModel(var wordTopicCounts: BigMatrix[Long],
               var topicCounts: BigVector[Long],
               val config: LDAConfig) extends Serializable {

  /**
    * Gets the top n words for each topic in the topic model
    *
    * @param words The number of words to return
    * @return Array of a list of words for each topic
    */
  def describe(words: Int)(implicit ec: ExecutionContext, timeout: Timeout): Array[List[(Long, Double)]] = {

    // Construct per-topic bounded priority queues
    val topWords = Array.fill(config.topics)(new BoundedPriorityQueue[(Double, Long)](words))
    val globalCounts = Await.result(topicCounts.pull((0L until config.topics).toArray), timeout.duration)

    // Iterate over blocks of rows and add entries to the bounded priority queue
    var start = 0L
    new RowBlockIterator[Long](wordTopicCounts, 10000).foreach {
      case rowBlock =>
        var i = 0
        while (i < rowBlock.length) {
          val row = rowBlock(i)
          var t = 0
          while (t < config.topics) {
            val p = (config.β + row(t).toDouble) / (config.vocabularyTerms * config.β + globalCounts(t).toDouble)
            topWords(t).enqueue((p, start + i))
            t += 1
          }
          i += 1
        }
        start += rowBlock.length
    }

    // Get final data from the bounded priority queue and convert it to the correct format
    topWords.map {
      case bpq => bpq.iterator.toList.sorted.reverse.map { case (prob, f) => (f, prob) }
    }
  }

  /**
    * Writes the topic model to given file
    *
    * @param file The file
    * @param ec The execution context in which to execute requests to the parameter server
    * @param timeout The timeout of operations to the parameter server
    */
  def writeProbabilitiesToCSV(file: File)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {

    // Get global counts
    val globalCounts = Await.result(topicCounts.pull((0L until config.topics).toArray), timeout.duration)
    val writer = new PrintWriter(file)

    // Iterate over blocks of rows and add entries to the bounded priority queue
    var start = 0L
    new RowBlockIterator[Long](wordTopicCounts, 10000).foreach {
      case rowBlock =>
        var i = 0
        while (i < rowBlock.length) {
          writer.print(s"${start + i}")
          val row = rowBlock(i)
          var topic = 0
          while (topic < config.topics) {
            val probability = (config.β + row(topic).toDouble) / (config.vocabularyTerms * config.β + globalCounts(topic).toDouble)
            writer.print(s" $probability")
            topic += 1
          }
          writer.println()
          i += 1
        }
        start += rowBlock.length
    }

    // Close file
    writer.close()
  }

  /**
    * Writes the LDA model to a file so it can be restored later
    *
    * @param path The path where to store the binary topic model
    */
  def writeToFile(path: String)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    val os = new ObjectOutputStream(new FileOutputStream(path))
    os.writeObject(config)
    val globalCounts = Await.result(topicCounts.pull((0L until config.topics).toArray), timeout.duration)
    os.writeObject(globalCounts)
    new RowBlockIterator[Long](wordTopicCounts, 10000).foreach {
      case rowBlock =>
        var i = 0
        while (i < rowBlock.length) {
          os.writeObject(rowBlock(i))
          i += 1
        }
    }
    os.close()
  }

}

object LDAModel {

  /**
    * Constructs an empty LDA model based on given configuration
    *
    * @param config The LDA configuration
    */
  def apply(gc: Client, config: LDAConfig): LDAModel = {
    //val topicWordCounts = gc.matrix[Long](config.vocabularyTerms, config.topics, 2, (x,y) => CyclicPartitioner(x, y))
    val topicWordCounts = gc.matrix[Long](config.powerlawCutoff, config.topics, 2, (x,y) => CyclicPartitioner(x, y))
    val granularTopicWordCounts = new GranularBigMatrix[Long](topicWordCounts, 120000)
    val globalCounts = new RetryBigVector[Long](gc.vector[Long](config.topics, 1), 5)
    new LDAModel(new RetryBigMatrix[Long](granularTopicWordCounts, 5), globalCounts, config)
  }

  /**
    * Converts a Spark LocalLDAModel to a glint model
    * @param sparkModel The spark model
    * @param gc The glint client
    * @param config The configuration
    * @return The glint model
    */
  def fromSpark(sparkModel: LocalLDAModel, gc: Client, config: LDAConfig): LDAModel = {
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(300 seconds)

    val model = apply(gc, config)
    val buff = new BufferedBigMatrix[Long](model.wordTopicCounts, 100000)
    val globs = new Array[Long](config.topics)
    var i = 0
    val lock = new SimpleLock(16)
    while (i < sparkModel.topicsMatrix.numRows) {
      var j = 0
      while (j < sparkModel.topicsMatrix.numCols) {
        val value = sparkModel.topicsMatrix(i, j).toLong
        buff.pushToBuffer(i, j, value)
        if (buff.isFull) {
          lock.acquire()
          buff.flush().onComplete(_ => lock.release())
        }
        globs(j) += value
        j += 1
      }
      i += 1
    }
    lock.acquire()
    buff.flush().onComplete(_ => lock.release())
    lock.acquire()
    model.topicCounts.push((0L until config.topics).toArray, globs).onComplete(_ => lock.release())
    lock.acquireAll()
    lock.releaseAll()
    model
  }

  /**
    * Reads an LDA model from a file and stores it on the parameter servers
    *
    * @param path Where to load the model from
    * @return The LDA model
    */
  def readFromFile(path: String, gc: Client): LDAModel = {

    // Set up input stream and read config
    val is = new ObjectInputStream(new FileInputStream(path))
    val config = is.readObject().asInstanceOf[LDAConfig]

    // Create a model with given configuration
    val model = LDAModel(gc, config)
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(300 seconds)

    // Construct buffer
    val buffer = new BufferedBigMatrix[Long](model.wordTopicCounts, 1000000)

    // Loop and read data while pushing it to the parameter server
    val flushLock = new SimpleLock(16)
    var i = 0L
    while (i < config.vocabularyTerms) {
      val row = is.readObject().asInstanceOf[Array[Long]]
      var j = 0
      while (j < row.length) {
        buffer.pushToBuffer(i, j, row(j))
        if (buffer.isFull) {
          flushLock.acquire()
          buffer.flush().onComplete(_ => flushLock.release())
        }
        j += 1
      }
      i += 1
    }

    // Wait for all transactions to finish
    flushLock.acquireAll()
    flushLock.releaseAll()

    // Return the LDA model
    model
  }

}

