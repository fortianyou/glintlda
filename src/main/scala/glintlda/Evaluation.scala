package glintlda

import akka.util.Timeout
import breeze.numerics._
import com.typesafe.scalalogging.slf4j.Logger
import glint.iterators.RowBlockIterator
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Handles evaluation
  *
  * @param config The LDA configuration
  */
class Evaluation(config: LDAConfig) extends Serializable {

  /**
    * Aggregate evaluation function for gibbs samples
    * Computes local document log likelihood and token counts and aggregates them together
    *
    * @param current The current count
    * @return The new count
    */
  def aggregateDocument(current: (Double, Long), freqSample: FreqAwareGibbsSample): (Double, Long) = {

    val sample = freqSample.freqSample
    // Initialize
    val localTokenCounts: Long = sample.features.length
    var localDocLikelihood: Double = 0.0

    // Add all document-dependent likelihood computations
    localDocLikelihood += lgamma(config.α * config.topics)
    localDocLikelihood -= lgamma(config.α * config.topics + sample.features.length)
    val sparseCounts = sample.sparseCounts(config.topics)
    var offset = 0
    while (offset < sparseCounts.activeSize) {
      val value: Int = sparseCounts.valueAt(offset)
      localDocLikelihood += lgamma(config.α + value)
      offset += 1
    }
    localDocLikelihood += (config.topics - sparseCounts.activeSize) * lgamma(config.α)
    localDocLikelihood -= config.topics * lgamma(config.α)

    // Return result
    (current._1 + localDocLikelihood, current._2 + localTokenCounts)
  }

  /**
    * Aggregates results from document log likelihood and token counts
    *
    * @param first The first result
    * @param second The second result
    * @return The final results
    */
  def aggregateResults(first: (Double, Long), second: (Double, Long)): (Double, Long) = {
    (first._1 + second._1, first._2 + second._2)
  }

  /**
    * Computes the word log likelihood of given LDA model
    *
    * @param model The model
    * @param ec The execution context
    * @param timeout The timeouts
    * @return The word log likelihood
    */
  private def computeWordLoglikelihood(model: LDAModel)(implicit ec: ExecutionContext, timeout: Timeout): Double = {

    // Construct necessary variables
    var wordLikelihood = 0.0

    // Start iterating over rows of coordinates
    var start = 0
    new RowBlockIterator[Long](model.wordTopicCounts, model.config.blockSize).foreach {
      case rowBlock =>
        var i = 0
        while (i < rowBlock.length) {
          var j = 0
          while (j < rowBlock(i).length) {
           // println(s"view: $wordLikelihood ${rowBlock(i)(j).toDouble}")
            wordLikelihood += lgamma(config.β + rowBlock(i)(j).toDouble)
            wordLikelihood -= lgamma(config.β)
            j += 1
          }
          i += 1
        }
        start += rowBlock.length
    }

    //println(s"Un-normalized likelihood: ${wordLikelihood}")

    // Normalize
    val global = Await.result(model.topicCounts.pull((0L until config.topics).toArray), timeout.duration)
    var i = 0
    while (i < global.length) {
     // println(s"${global(i).toDouble} ${lgamma(config.vocabularyTerms * config.β)}   ${lgamma(config.vocabularyTerms * config.β + global(i).toDouble)}")
      wordLikelihood += lgamma(config.vocabularyTerms * config.β)
      wordLikelihood -= lgamma(config.vocabularyTerms * config.β + global(i).toDouble)
      i += 1
    }

    // Return result
    wordLikelihood
  }

  /**
    * Logs current state of the model
    *
    * @param iteration The current iteration
    * @param docLoglikelihood The document log likelihood
    * @param tokenCounts The token counts
    * @param model The LDA model
    */
  def logCurrentState(iteration: Int, docLoglikelihood: Double, tokenCounts: Long, model: LDAModel): Unit = {

    // Construct necessary variables for pipelined communication with parameter server
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(300 seconds)

    // Get the independently computed log likelihood numbers
    val wordLoglikelihood = computeWordLoglikelihood(model)
    val loglikelihood = docLoglikelihood + wordLoglikelihood

    // Compute perplexity
    val perplexity = Math.exp(-loglikelihood / tokenCounts)

    // Print to log
    val logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}")
    logger.info(s"Evaluation after iteration ${iteration}")
    logger.info(s"Doc log-likelihood:  ${docLoglikelihood}")
    logger.info(s"Word log-likelihood: ${wordLoglikelihood}")
    logger.info(s"Log-likelihood:      ${loglikelihood}")
    logger.info(s"Token counts:        ${tokenCounts}")
    logger.info(s"Perplexity:          ${perplexity}")

  }

}
