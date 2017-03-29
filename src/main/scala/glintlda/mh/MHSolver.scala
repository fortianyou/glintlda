package glintlda.mh

import breeze.linalg.{DenseVector, SparseVector, Vector}
import glint.iterators.RowBlockIterator
import glint.models.client.buffered.BufferedBigMatrix
import glintlda.util.{AggregateBuffer, FastRNG, SimpleLock, Timer, time}
import glintlda.{FreqAwareGibbsSample, GibbsSample, LDAModel, Solver}
import you.dataserver.DataServerClient

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * A block-coordinate metropolis-hastings based solver
  *
  * @param model The LDA model
  * @param id The identifier
  */
class MHSolver(model: LDAModel, id: Int) extends Solver(model, id) {

  var nwt: mutable.HashMap[Int, mutable.Map[Int, Int]] = null
  val bufferSize = 100000
  val lock = new SimpleLock(16, logger)
  var globalSummary: Array[Double] = null
  var globalAlias: AliasTable = null
  val nOfTopics = model.config.topics

  // Create buffer
  //val aggregateBuffer = new AggregateBuffer(model.config.powerlawCutoff, model.config)
  val bufferGlobal = new Array[Long](model.config.topics)
  val buffer = new BufferedBigMatrix[Long](model.wordTopicCounts, bufferSize)

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples   The samples to run the algorithm on
    * @param iteration The iteration number
    */
  override protected def fit(samples: Array[FreqAwareGibbsSample], iteration: Int): Unit = {
    time(logger, "fit use time: ") {
      // Create random and sampler
      val random = new FastRNG(model.config.seed + id)
      val sampler = new Sampler(model.config, model.config.mhSteps, random)

      // Pull global topic counts
      val global: Array[Long] = Await.result(model.topicCounts.pull((0L until model.config.topics).toArray), 300 seconds)
      val βum = model.config.vocabularyTerms * model.config.β
      globalSummary = global.map(_ + βum)


      val priorStats = DenseVector.zeros[Double](model.config.topics)
      for (i <- 0 until model.config.topics) {
        priorStats(i) = model.config.β / globalSummary(i)
      }

      globalAlias = new AliasTable(priorStats)
      // Initialize variables used during iteration of model slices
      var start: Int = 0
      var end: Int = 0

      val freqSamples = samples.map(_.freqSample)
      val unFreqSamples = samples.map(_.unFreqSample)
      val unFreqVocab = new mutable.HashSet[Int]()
      val ndt = new mutable.HashMap[Int, mutable.Map[Int, Int]]()


      unFreqSamples.foreach {
        sample =>
          unFreqVocab ++= sample.features
          val nt = mutable.Map[Int, Int]()
          ndt(sample.docId) = nt
          sample.topics.foreach {
            topic =>
              if (!nt.contains(topic)) nt(topic) = 0
              nt(topic) += 1
          }
      }

      nwt = model.sparsePS.pull(unFreqVocab.toArray)

      var zeroCount = 0
      var negCount = 0
      var posCount = 0
      var ret = model.sparsePS.incKey("stage.sparse")
      time(logger, "Del sparse values: ") {
        if (ret == model.config.partitions) {
          nwt.foreach {
            case (word, nt) =>
              nt.foreach {
                case (t, v) =>
                  if (v == 0) {
                    if (model.sparsePS.isDelBuffFull) {
                      model.sparsePS.flushDelBuffer()
                    }
                    model.sparsePS.delBufferred(word, t)
                    zeroCount += 1
                  } else if (v < 0) {
                    negCount +=1
                  } else {
                    posCount += 1
                  }
              }
          }
          model.sparsePS.flushDelBuffer()
        } else {
           nwt.foreach {
            case (word, nt) =>
              nt.foreach {
                case (t, v) =>
                  if (v == 0) {
                    zeroCount += 1
                  } else if (v < 0) {
                    negCount +=1
                  } else {
                    posCount += 1
                  }
              }
          }
          while (ret != model.config.partitions) {
            Thread.sleep(100)
            ret = model.sparsePS.getKey("stage.sparse")
          }
        }
      }


      logger.info(s"posCount = $posCount, zeroCount = $zeroCount, negCount = $negCount")

      var rowWait = System.currentTimeMillis()

      var tot_blocks = 0
      var totReSampleTime: Long = 0
      var totBlockReadTime: Long = 0
      val tot_time = time() {
        logger.info(s"matrix size: ${model.wordTopicCounts.rows} * ${model.wordTopicCounts.cols}")
        sampler.stageDensity = true
        time(logger, "Density sample use time: ") {
          // Iterate over blocks of rows of the word topic count matrix
          val block = new RowBlockIterator[Long](model.wordTopicCounts, model.config.blockSize).foreach {
            case rowBlock =>
              totBlockReadTime += System.currentTimeMillis() - rowWait
              //  logger.info(s"Row block wait time: ${System.currentTimeMillis() - rowWait}ms")
              tot_blocks += 1
              // Reset flush lock time
              lock.waitTime = 0

              // Perform resampling on just this block of rows from the word topic count matrix
              end += rowBlock.length
              // logger.info(s"Resampling features [${start}, ..., ${end})")

              // Compute alias tables
              val aliasTables = //time(logger, "Alias time: ") {
              computeAliasTables(rowBlock)
              // }

              // Perform resampling
              //time(logger, "Resampling time: ") {
              totReSampleTime += time() {
                resample(freqSamples, sampler, global, rowBlock, aliasTables, ndt, start, end)
              }

              // Increment start index for next block of rows
              start += rowBlock.length
              rowWait = System.currentTimeMillis()

          }
        }

        time(logger, "Sparse sample use time: ") {
          val aliasTables = computeAliasTables2(nwt)
          sampler.stageDensity = false
          resample(samples.filter(_.freqSample.hasUnFreq), sampler, global, nwt, aliasTables, ndt)
        }
      }

      lock.acquire()
      val flushGlobal = model.topicCounts.push((0L until model.config.topics).toArray, bufferGlobal)
    flushGlobal.onComplete(_ => lock.release())
    flushGlobal.onFailure { case ex => println(ex.getMessage + "\n" + ex.getStackTraceString) }


      logger.info(s"each block use time: ${tot_time / 1000000 / tot_blocks}ms, total ${tot_blocks} blocks")
      logger.info(s"total re-sample time: ${totReSampleTime / 1000000}, total block read time: ${totBlockReadTime}")
      // Wait until all changes have succesfully propagated to the parameter server before finishing this iteration

      // Wait until all changes have succesfully propagated to the parameter server before finishing this iteration
      logger.info(s"Waiting for transfers to finish")
      lock.acquireAll()
      lock.releaseAll()
    }

  }

  /**
    * Computes alias tables for given block of features
    *
    * @param block The block of features
    * @return The alias tables
    */
  def computeAliasTables(block: Array[Vector[Long]]): Array[AliasTable] = {
    val aliasTables = new Array[AliasTable](block.length)
    var k = 0
    while (k < block.length) {
      val probs = block(k).map(_.toDouble + model.config.β)
      /*
            for (i <- 0 until nOfTopics) {
              probs(i) /= globalSummary(i)
            }*/
      aliasTables(k) = new AliasTable(probs)

      k += 1
    }
    aliasTables
  }

  def computeAliasTables2(nwt: mutable.HashMap[Int, mutable.Map[Int, Int]])
  : mutable.HashMap[Int, AliasTable] = {
    nwt.map {
      case (word, nt) =>
        val sparseCounts = SparseVector.zeros[Double](model.config.topics)
        nt.foreach {
          case (k, v) =>
            sparseCounts(k) = v / globalSummary(k)
        }

        (word, new AliasTable(sparseCounts, globalAlias))
    }
  }

  /**
    * Resamples given samples with
    *
    * @param samples           The samples
    * @param sampler           The sampler
    * @param global            The global topic counts
    */
  def resample(samples: Array[FreqAwareGibbsSample],
               sampler: Sampler,
               global: Array[Long],
               nwt: mutable.HashMap[Int, mutable.Map[Int, Int]],
               aliasTables: mutable.HashMap[Int, AliasTable],
               ndt: mutable.HashMap[Int, mutable.Map[Int, Int]]): Unit = {

    // Store global counts in sampler
    sampler.globalCounts = global

    // Iterate over documents, resampling each one
    var i = 0

    while (i < samples.length) {
      // Get sample and store appropriate counts in the sampler
      val freqAwareSample = samples(i)

      sampler.documentCounts = freqAwareSample.freqSample.denseCounts(model.config.topics)

      val sample = freqAwareSample.unFreqSample

      /** GTY: amend the count of un-frequency words **/
      ndt(sample.docId).foreach {
        case (topic, cnt) =>
          sampler.documentCounts(topic) += cnt
      }

      sampler.documentSize = sample.features.length
      sampler.documentTopicAssignments = sample.topics

      // Iterate over features
      var j = 0
      while (j < sample.features.length) {
        // If feature is in the current working set of features we perform actual resampling
        val feature = sample.features(j)
        val oldTopic = sample.topics(j)

        // Resample feature
        sampler.wordCountHash = nwt(feature)
        sampler.aliasTable = aliasTables(feature)
        val newTopic = sampler.sampleFeature(feature, oldTopic)
        sampleCore(sample, sampler, j, feature, newTopic, oldTopic)

        j += 1
      }

      i += 1
    }

    model.sparsePS.flushIncBuffer()
     // Flush buffer to push changes to word topic counts
    flushBuffer(buffer, lock)
  }

  /**
    * Resamples given samples with
    *
    * @param samples           The samples
    * @param sampler           The sampler
    * @param global            The global topic counts
    * @param block             The block of features
    * @param start             The index of the first feature
    * @param end               The index of the first non-included feature
    */
  def resample(samples: Array[GibbsSample],
               sampler: Sampler,
               global: Array[Long],
               block: Array[Vector[Long]],
               aliasTables: Array[AliasTable],
               ndt: mutable.HashMap[Int, mutable.Map[Int, Int]],
               start: Int,
               end: Int): Unit = {

    // Store global counts in sampler
    sampler.globalCounts = global

    // Iterate over documents, resampling each one
    var i = 0

    while (i < samples.length) {
      // Get sample and store appropriate counts in the sampler
      val sample = samples(i)
      sampler.documentCounts = sample.denseCounts(model.config.topics)

      /** GTY: amend the count of un-frequency words **/
      if (sample.hasUnFreq) {
        ndt(sample.docId).foreach {
          case (topic, cnt) =>
            sampler.documentCounts(topic) += cnt
        }
      }

      sampler.documentSize = sample.features.length
      sampler.documentTopicAssignments = sample.topics

      // Iterate over features
      var j = 0
      while (j < sample.features.length) {
        // If feature is in the current working set of features we perform actual resampling
        val feature = sample.features(j)
        val oldTopic = sample.topics(j)
        if (feature >= start && feature < end) {

          // Resample feature
          sampler.wordCountVec = block(feature - start)
          sampler.aliasTable = aliasTables(feature - start)
          val newTopic = sampler.sampleFeature(feature, oldTopic)
          sampleCore(sample, sampler, j, feature, newTopic, oldTopic)
        }


        j += 1
      }

      i += 1
    }

    // Flush buffer to push changes to word topic counts
    flushBuffer(buffer, lock)
  }

  private def sampleCore(sample: GibbsSample, sampler: Sampler, j: Int, feature: Int, newTopic: Int, oldTopic: Int): Unit = {

    // Topic has changed, update the necessary counts
    if (oldTopic != newTopic) {
      sample.topics(j) = newTopic
      sampler.documentCounts(oldTopic) -= 1
      sampler.documentCounts(newTopic) += 1

      sampler.wordCountVec(oldTopic) -= 1
      sampler.wordCountVec(newTopic) += 1
      sampler.globalCounts(oldTopic) -= 1
      sampler.globalCounts(newTopic) += 1

      if (sampler.stageDensity) {
        // Add to buffer and flush if necessary
        buffer.pushToBuffer(feature, oldTopic, -1)
        flushBufferIfFull(buffer, lock)
        buffer.pushToBuffer(feature, newTopic, 1)
        flushBufferIfFull(buffer, lock)
        //}

      } else {
        if (model.sparsePS.isIncBuffFull) model.sparsePS.flushIncBuffer()
        //TODO: 稀疏服务器的更新
        model.sparsePS.increaseBufferred(feature, oldTopic, -1)
        model.sparsePS.increaseBufferred(feature, newTopic, 1)
        nwt(feature)(oldTopic) -= 1
        if (!nwt(feature).contains(newTopic)) nwt(feature)(newTopic) = 0
        nwt(feature)(newTopic) += 1
      }

      bufferGlobal(oldTopic) -= 1
      bufferGlobal(newTopic) += 1
    }

  }

  /**
    * Attempts to flush the buffer if it is full
    *
    * @param buffer The buffer
    * @param lock The semaphore lock to act as a back-pressure
    * @tparam V The type of values stored in the buffer
    */
  @inline
  private def flushBufferIfFull[V](buffer: BufferedBigMatrix[V], lock: SimpleLock): Unit = {
    if (buffer.isFull) {
      flushBuffer(buffer, lock)
    }
  }

  /**
    * Flushes the buffer
    *
    * @param buffer The buffer
    * @param lock The semaphore lock to act as back-pressure
    * @tparam V The type of values stored in the buffer
    */
  @inline
  private def flushBuffer[V](buffer: BufferedBigMatrix[V], lock: SimpleLock): Unit = {
    lock.acquire()
    val flush = buffer.flush()
    flush.onComplete(_ => lock.release())
    flush.onFailure { case ex => logger.error(ex.getMessage + "\n" + ex.getStackTraceString) }
  }

  /**
    * Runs the LDA inference algorithm on given partition of the data without
    * updating topic-word counts
    *
    * @param samples The samples to run the algorithm on
    */
  override protected def test(samples: Array[GibbsSample]): Unit =  null
}
