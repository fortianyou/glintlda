package glintlda.mh

import breeze.linalg.{SparseVector, Vector}
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

  val bufferSize = 100000
  val lock = new SimpleLock(16, logger)
  var globalSummary: Array[Long] = null
  val docTopicDelta = new mutable.HashMap[Int, Int]()
  val nOfTopics = model.config.topics

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    * @param iteration The iteration number
    */
  override protected def fit(samples: Array[GibbsSample], iteration: Int): Unit = {
    time(logger, "fit use time: ") {
      // Create random and sampler
      val random = new FastRNG(model.config.seed + id)
      val sampler = new Sampler(model.config, model.config.mhSteps, random)

      // Pull global topic counts
      val global: Array[Long] = Await.result(model.topicCounts.pull((0L until model.config.topics).toArray), 300 seconds)
      globalSummary = global

      // Initialize variables used during iteration of model slices
      var start: Int = 0
      var end: Int = 0

        /** GTY **/
      val docKeys = time(logger, "filter un frequency keys: ") {
         samples.filter(_.hasUnFreq).map(_.docId)
      }

      logger.info(s"docKeys size: ${docKeys.length}")
      val ndt = time(logger, "Pull data server wait time: ") {
        ds.pull(docKeys)
      }
      logger.info(s"ndt size: ${ndt.size}")

      var rowWait = System.currentTimeMillis()

      var tot_blocks = 0
      var totReSampleTime:Long = 0
      var totBlockReadTime: Long = 0
      var totDsWaitTime:Long = 0
      val tot_time = time() {
        logger.info(s"matrix size: ${model.wordTopicCounts.rows} * ${model.wordTopicCounts.cols}")
        // Iterate over blocks of rows of the word topic count matrix
        new RowBlockIterator[Long](model.wordTopicCounts, model.config.blockSize).foreach {
          case rowBlock =>
            totBlockReadTime += System.currentTimeMillis() - rowWait
            //  logger.info(s"Row block wait time: ${System.currentTimeMillis() - rowWait}ms")
            tot_blocks +=1
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
               totDsWaitTime += resample(samples, sampler, global, rowBlock, aliasTables, ndt, start, end)
            }

            // Log flush lock wait times
            //       logger.info(s"Flush lock wait time: ${lock.waitTime}ms")

            // Increment start index for next block of rows
            start += rowBlock.length
            rowWait = System.currentTimeMillis()

        }

      }


    if (model.config.powerlawCutoff != model.config.vocabularyTerms) {
        ds.flushIncBuffer()
        ds.flushDelBuffer()
    }


      logger.info(s"each block use time: ${tot_time/1000000/tot_blocks}ms, total ${tot_blocks} blocks")
      logger.info(s"total re-sample time: ${totReSampleTime/1000000}, total block read time: ${totBlockReadTime}")
      logger.info(s"total ds wait time: ${totDsWaitTime/1000000}")
      // Wait until all changes have succesfully propagated to the parameter server before finishing this iteration

      // Wait until all changes have succesfully propagated to the parameter server before finishing this iteration
      logger.info(s"Waiting for transfers to finish")
      lock.acquireAll()
      lock.releaseAll()
    }

  }

  /**
    * Runs the LDA inference algorithm on given partition of the data without
    * updating the word-topic counts
    *
    * @param samples The samples to run the algorithm on
    */
  override protected def test(samples: Array[GibbsSample]): Unit = {

    // Create random and sampler
    val random = new FastRNG(model.config.seed + id)
    val sampler = new Sampler(model.config, model.config.mhSteps, random)

    // Sampler should not infer but only test
    sampler.infer = 0

    // Pull global topic counts
    val global = Await.result(model.topicCounts.pull((0L until model.config.topics).toArray), 300 seconds)

    // Initialize variables used during iteration of model slices
    var start: Int = 0
    var end: Int = 0

    val ndt = time(logger, "Pull data server wait time: ") {
      /** GTY **/
      val docKeys = samples.map(_.docId)
      ds.pull(docKeys)
    }

    var rowWait = System.currentTimeMillis()
    // Iterate over blocks of rows of the word topic count matrix
    new RowBlockIterator[Long](model.wordTopicCounts, model.config.blockSize).foreach {
      case rowBlock =>
        logger.info(s"Row block wait time: ${System.currentTimeMillis() - rowWait}ms")

        // Reset flush lock time
        lock.waitTime = 0

        // Perform resampling on just this block of rows from the word topic count matrix
        end += rowBlock.length
        logger.info(s"Resampling features [${start}, ..., ${end})")

        // Compute alias tables
        val aliasTables = time(logger, "Alias time: ") {
          computeAliasTables(rowBlock)
        }

        // Perform resampling without updating global counts
        time(logger, "Resampling time: ") {
          resample(samples, sampler, global, rowBlock, aliasTables, ndt, start, end, false)
        }

        // Increment start index for next block of rows
        start += rowBlock.length
        rowWait = System.currentTimeMillis()
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

  /**
    * Resamples given samples with
    *
    * @param samples The samples
    * @param sampler The sampler
    * @param global The global topic counts
    * @param block The block of features
    * @param start The index of the first feature
    * @param end The index of the first non-included feature
    * @param shouldUpdateModel A boolean indicating whether the resampling should update the model (default: true)
    */
  def resample(samples: Array[GibbsSample],
               sampler: Sampler,
               global: Array[Long],
               block: Array[Vector[Long]],
               aliasTables: Array[AliasTable],
               ndt: mutable.HashMap[Int, mutable.Map[Int, Int]],
               start: Int,
               end: Int,
               shouldUpdateModel: Boolean = true): Long = {

    // Create buffer
    //val aggregateBuffer = new AggregateBuffer(model.config.powerlawCutoff, model.config)
    val bufferGlobal = new Array[Long](model.config.topics)
    val buffer = new BufferedBigMatrix[Long](model.wordTopicCounts, bufferSize)

    var total_time: Long = 0

    // Store global counts in sampler
    sampler.globalCounts = global

    // Iterate over documents, resampling each one
    var i = 0

    val timer = new Timer()
    while (i < samples.length) {
      docTopicDelta.clear()
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

      /** GTY **/


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
          sampler.wordCounts = block(feature - start)
          sampler.aliasTable = aliasTables(feature - start)
          val newTopic = sampler.sampleFeature(feature, oldTopic)

          // Topic has changed, update the necessary counts
          if (oldTopic != newTopic) {
            sample.topics(j) = newTopic
            sampler.documentCounts(oldTopic) -= 1
            sampler.documentCounts(newTopic) += 1

            if (shouldUpdateModel) {
              sampler.wordCounts(oldTopic) -= 1
              sampler.wordCounts(newTopic) += 1
              sampler.globalCounts(oldTopic) -= 1
              sampler.globalCounts(newTopic) += 1

              total_time += time() {
                if (sample.hasUnFreq) {
                  if (!docTopicDelta.contains(oldTopic)) {
                    docTopicDelta(oldTopic) = 0
                  }
                  if (!docTopicDelta.contains(newTopic)) {
                    docTopicDelta(newTopic) = 0
                  }
                  docTopicDelta(oldTopic) -= 1
                  docTopicDelta(newTopic) += 1
                }
              }
              // Add to buffer and flush if necessary
              buffer.pushToBuffer(feature, oldTopic, -1)
              flushBufferIfFull(buffer, lock)
              buffer.pushToBuffer(feature, newTopic, 1)
              flushBufferIfFull(buffer, lock)
              //}

              bufferGlobal(oldTopic) -= 1
              bufferGlobal(newTopic) += 1
            }

          }
        }


        j += 1
      }

      if (sample.hasUnFreq) {
        total_time += time() {

          docTopicDelta.foreach {
            case (k, v) =>
              if (sampler.documentCounts(k) + v == 0) {
                ds.delBufferred(sample.docId, k)
              } else {
                ds.increaseBufferred(sample.docId, k, v)
              }
          }
        }
      }

      i += 1
    }

    //logger.info(s"Iterate over the corpus use time: " + timer.getReadableRunnningTime())

    //logger.info(s"Update data server wait time: ${total_time / 1000000}ms")
   // time(logger, "push data use time: ") {
      // Flush powerlaw buffer
    //  time(logger, "lock acquire use time: ") {
        /*time(logger, "flush aggregate buffer use time: ") {
          logger.info(s"n(aggregate) = ${aggregateBuffer.size}, sum(aggregate) = ${aggregateBuffer.buffer.sum}," +
            s" nozero(aggregate) = ${aggregateBuffer.buffer.filter(_ != 0).size}")

          aggregateBuffer.flush(model.wordTopicCounts).onComplete(_ => lock.release())
        }*/

        // Flush buffer to push changes to word topic counts
        flushBuffer(buffer, lock)
        lock.acquire()
     // }
      //logger.info(s"n(global) = ${bufferGlobal.length}, sum(global) = ${bufferGlobal.sum}," +
      //  s" nozero(global) = ${bufferGlobal.filter(_ != 0).size}")
      val flushGlobal = model.topicCounts.push((0L until model.config.topics).toArray, bufferGlobal)
      flushGlobal.onComplete(_ => lock.release())
      flushGlobal.onFailure { case ex => println(ex.getMessage + "\n" + ex.getStackTraceString) }
    //}

    total_time
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

}
