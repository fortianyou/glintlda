package glintlda.mh

import breeze.linalg.Vector
import scala.collection.mutable
import glintlda.LDAConfig
import glintlda.util.FastRNG

/**
  * A metropolis hastings sampler based on the LightLDA implementation
  * Yuan, Jinhui, et al. "LightLDA: Big Topic Models on Modest Computer Clusters.", 2015
  *
  * @param config The LDA configuration
  * @param mhSteps The number of metropolis-hastings steps
  * @param random The random number generator
  */
class Sampler(config: LDAConfig, mhSteps: Int = 2, random: FastRNG) {

  private val α = config.α
  private val β = config.β
  private val αSum = config.topics * α
  private val βSum = config.vocabularyTerms * config.β

  var infer: Int = 1
  var aliasTable: AliasTable = null
  var wordCountVec: Vector[Long] = null
  var wordCountHash: mutable.Map[Int, Int] = null
  var stageDensity = true
  var globalCounts: Array[Long] = null
  var documentCounts: Vector[Int] = null
  var documentTopicAssignments: Array[Int] = null
  var documentSize: Int = 0

  /**
    * Produces a new topic for given feature and old topic
    *
    * @param feature The feature
    * @param oldTopic The old topic
    * @return
    */
  def sampleFeature(feature: Int, oldTopic: Int): Int = {
    if (!stageDensity) return sampleFeatureSimple(feature, oldTopic)

    var s: Int = oldTopic
    var mh: Int = 0

    // Each Metropolis-Hastings step alternates between a word proposal and doc proposal
    while(mh < mhSteps) {

      // Set t
      var t: Int = s

      // Word Proposal
      t = aliasTable.draw(random)
      if (t != s) {
        var docS = documentCounts(s) + α
        var docT = documentCounts(t) + α
        var wordS =  wordCountVec(s)  +  β
        var wordT =  wordCountVec(t)  +  β
        var globalS = globalCounts(s) + βSum
        var globalT = globalCounts(t) + βSum

        val proposalS = wordS / globalS
        val proposalT = wordT / globalT

        if (s == oldTopic) {
          docS -= 1
          wordS -= infer
          globalS -= infer
        }
        if (t == oldTopic) {
          docT -= 1
          wordT -= infer
          globalT -= infer
        }

        val pi = (docT * wordT * globalS * proposalS) / (docS * wordS * globalT * proposalT)
        if (random.nextDouble() < pi) {
          s = t
        }
      }

      // Document proposal
      val pickOrExplore = random.nextDouble() * (documentSize + αSum)
      if (pickOrExplore < documentSize) {
        t = documentTopicAssignments(pickOrExplore.toInt)
      } else {
        t = random.nextPositiveInt() % config.topics
      }
      if (t != s) {
        var docS = documentCounts(s) + α
        var docT = documentCounts(t) + α
        var wordS = (if(stageDensity) wordCountVec(s) else wordCountHash(s)) + β
        var wordT = (if(stageDensity) wordCountVec(t) else wordCountHash(t)) + β
        var globalS = globalCounts(s) + βSum
        var globalT = globalCounts(t) + βSum

        val proposalS = docS
        val proposalT = docT

        if (s == oldTopic) {
          docS -= 1
          wordS -= infer
          globalS -= infer
        }
        if (t == oldTopic) {
          docT -= 1
          wordT -= infer
          globalT -= infer
        }

        val pi = (docT * wordT * globalS * proposalS) / (docS * wordS * globalT * proposalT)
        if (random.nextDouble() < pi) {
          s = t
        }
      }

      mh += 1
    }
    s
  }


  def sampleFeatureSimple(feature: Int, oldTopic: Int): Int = {
    var s = oldTopic
    // Word Proposal
    val t = aliasTable.draw(random)
    var docS = documentCounts(s) + α
    var docT = documentCounts(t) + α
    var wordS = (if (!wordCountHash.contains(s)) 0 else wordCountHash(s)) + β
    var wordT = (if (!wordCountHash.contains(t)) 0 else wordCountHash(t)) + β
    var globalS = globalCounts(s) + βSum
    var globalT = globalCounts(t) + βSum

    val proposalS = wordS / globalS
    val proposalT = wordT / globalT

    if (s == oldTopic) {
      docS -= 1
      wordS -= infer
      globalS -= infer
    }
    if (t == oldTopic) {
      docT -= 1
      wordT -= infer
      globalT -= infer
    }

    val pi = (docT * wordT * globalS * proposalS) / (docS * wordS * globalT * proposalT)
    if (random.nextDouble() < pi) {
      s = t
    }

    s
  }

}
