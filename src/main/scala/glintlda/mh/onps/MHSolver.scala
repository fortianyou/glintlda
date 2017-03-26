package glintlda.mh.onps

import breeze.linalg.{SparseVector, Vector}
import glintlda.mh.AliasTable
import glintlda.util.{AggregateBuffer, FastRNG, SimpleLock, time}
import glintlda.{FreqAwareGibbsSample, LDAModel, Solver, WDReverseGibbsSample}
import you.dataserver.DataServerClient

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * A metropolis-hastings based solver
  *
  * @param model The LDA model
  */
class MHSolver(model: LDAModel){
  implicit protected val ec = ExecutionContext.Implicits.global
  protected val logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}")
  val nOfTopics = model.config.topics
  var globalSummary: Array[Long] = null
  val lock = new SimpleLock(16, logger)

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    */
  def fit(samples: mutable.HashMap[Int, WDReverseGibbsSample], locations: Map[Byte, String]): Unit = {
    val nWorks = locations.size
    // Create random and sampler
    val random = new FastRNG(model.config.seed)
    // Pull global topic counts
    val global = Await.result(model.topicCounts.pull((0L until model.config.topics).toArray), 300 seconds)
    val bufferGlobal = SparseVector.zeros[Int](model.config.topics)
    globalSummary = global

    val βsum = model.config.β*model.config.vocabularyTerms

    time(logger, "Ps side sampling wait time: ") {
      for (p <- locations.keySet) {
        val proposals = new ArrayBuffer[(Int, Int, Int, Int, Int, Double)]()
        samples.foreach {
          case (word, sample) =>

            val wordCount = sample.denseWordCounts(nOfTopics)
            val aliasTable = computeAliasTables(wordCount)

            for (i <- 0 until sample.features.length) {
              if (sample.localtion(i) == p) {
                val s = sample.topics(i)
                val t = aliasTable.draw(random)
                val d = sample.features(i)

                val wordS = wordCount(s) + model.config.β
                val wordT = wordCount(t) + model.config.β
                val globalS = global(s) + βsum
                val globalT = global(t) + βsum
                val propS = wordS / globalS
                val propT = wordT / globalT

                val partialPi = (wordT * globalS * propS) / (wordS * globalT * propT)
                proposals += ((word, d, i, s, t, partialPi))
              }
            }
        }

        val keys = proposals.flatMap(x => Array((x._2, x._4), (x._2, x._5))).distinct

        val ds = new DataServerClient[Int](host = locations(p))
        val ndt = new mutable.HashMap[Long, Int]()
        val results = ds.pull(keys.map(_._1), keys.map(_._2))
        for (i <- 0 until keys.length) {
          val key = keys(i)._1 * model.config.topics + keys(i)._2
          ndt(key) = results(i)
        }

        val deltaNdt = mutable.HashMap[(Int, Int), Int]()

        proposals.foreach {
          case (word, d, i, s, t, partialPi) =>
            val keyS = d * model.config.topics + s
            val keyT = d * model.config.topics + t
            val docS = ndt(keyS) - 1 + model.config.α
            val docT = ndt(keyT) - 1 + model.config.α
            val pi = partialPi * docT / docS

            if (random.nextDouble() < pi) {
              samples(word).topics(i) = t
              ndt(keyS) = ndt(keyS) - 1
              ndt(keyT) = ndt(keyT) + 1

              if (!deltaNdt.contains((d, s))) {
                deltaNdt.put((d, s), 0)
              }

              if (!deltaNdt.contains((d, t))) {
                deltaNdt.put((d, t), 0)
              }

              deltaNdt((d, s)) -= 1
              deltaNdt((d, t)) += 1
            }
        }


        deltaNdt.filter(_._2 != 0).foreach {
          case ((d, t), delta) =>
            val keyT = d * model.config.topics + t
            if (ndt(keyT) + delta == 0) {
              ds.delBufferred(d, t)
            } else {
              ds.increaseBufferred(d, t, delta)
            }

            bufferGlobal.update(t, bufferGlobal(t) + delta)
        }

        ds.flushIncBuffer()
        ds.flushDelBuffer()
      }
    }

    val indices = new ArrayBuffer[Long]()
    val values = new ArrayBuffer[Long]()

    for (i <- 0 until bufferGlobal.activeSize) {
      val value = bufferGlobal.valueAt(i)
      if (value != 0) {
        indices += bufferGlobal.indexAt(i)
        values += value
      }
    }

    lock.acquire()
    val flushGlobal = model.topicCounts.push(indices.toArray, values.toArray)
    flushGlobal.onComplete(_ => lock.release())
    flushGlobal.onFailure { case ex => println(ex.getMessage + "\n" + ex.getStackTraceString) }
    lock.acquireAll()
    lock.releaseAll()
  }



  def computeAliasTables(wordCounts: Vector[Int]): AliasTable = {
    val probs = wordCounts.map(_.toDouble + model.config.β)
    for (i <- 0 until nOfTopics) {
      probs(i) /= globalSummary(i)
    }

    new AliasTable(probs)
  }

}
