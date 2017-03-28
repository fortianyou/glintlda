package glintlda.mh

import breeze.linalg.{DenseVector, SparseVector, Vector, sum}
import glintlda.util.FastRNG

/**
  * An alias table for quickly drawing probabilities from an uneven distribution using Vose's method:
  * Vose, Michael D. "A linear algorithm for generating random numbers with a given distribution." 1991
  *
  * @param alias The aliases
  * @param prob The probabilities
  */
class AliasTable(alias: Array[Int], prob: Array[Double]) {

  var count: Int = 0
  var index: Array[Int] = null
  var aliasProb:Double = 0.0
  var aliasTable: AliasTable = null
  var len: Double = 0.0

  /**
    * Creates a new alias table for given vector of counts (interpreted as scaled probabilities)
    *
    * @param counts The counts
    */
  def this(counts: Vector[Double]) {
    this(new Array[Int](counts.length), new Array[Double](counts.length))
    init(counts)
  }

  def init(counts: Vector[Double]){

    val n = counts.length
    val countSum = sum(counts)
    len = countSum

    val small = new Array[Int](n)
    val large = new Array[Int](n)

    var l = 0
    var s = 0

    val p = new Array[Double](n)
    var i = 0
    while (i < n) {
      val pi = n * (counts(i) / countSum)
      p(i) = pi
      if (pi < 1.0) {
        small(s) = i
        s += 1
      } else {
        large(l) = i
        l += 1
      }
      i += 1
    }

    while (s != 0 && l != 0) {
      s = s - 1
      l = l - 1
      val j = small(s)
      val k = large(l)
      prob(j) = p(j)
      alias(j) = k
      p(k) = (p(k) + p(j)) - 1
      if (p(k) < 1.0) {
        small(s) = k
        s += 1
      } else {
        large(l) = k
        l += 1
      }
    }
    while (s > 0) {
      s -= 1
      prob(small(s)) = 1.0
    }
    while (l > 0) {
      l -= 1
      prob(large(l)) = 1.0
    }
  }

  def this(counts: SparseVector[Double], aliasTable: AliasTable) {
    this(new Array[Int](counts.activeSize), new Array[Double](counts.activeSize))
    val activeSize = counts.activeSize
    index = counts.index.slice(0, activeSize)
    val denseCounts: Vector[Double] = Vector.zeros[Double](activeSize)

    for (i <- 0 until activeSize) {
      denseCounts(i) = counts.valueAt(i)
    }

    init(denseCounts)
    this.aliasTable = aliasTable
    this.aliasProb = len / (len + this.aliasTable.len)
  }

  /**
    * Draws from the uneven multinomial probability distribution represented by this Alias table
    *
    * @param random The random number generator to use
    * @return A random outcome from the uneven multinomial probability distribution
    */
  def draw(random: FastRNG): Int = {
    if (index == null) {
      drawFromAlias(random)
    } else {
      if (random.nextDouble() < this.aliasProb) {
        val i = drawFromAlias(random)
        index(i)
      } else {
        this.aliasTable.draw(random)
      }
    }
  }

  private def drawFromAlias(random: FastRNG): Int =
  {
    val i = random.nextPositiveInt() % alias.length
    if (random.nextDouble() < prob(i)) {
      i
    } else {
      alias(i)
    }
  }

}
