package mh

import breeze.linalg.{DenseVector, SparseVector, sum}
import glintlda.mh.AliasTable
import glintlda.util.FastRNG
import org.scalatest.FunSuite

/**
  * Created by Roger on 17/3/25.
  */
class AliasTableSuite extends FunSuite{

  test("alias table") {
    val random = new FastRNG(1000)
    val weight: DenseVector[Int] = DenseVector.fill[Int](10)(random.nextPositiveInt() % 100)
    val tot = sum(weight)

    println(weight)
    val prob = weight.map( _.toDouble / tot)

    val alias = new AliasTable(weight.map(_.toDouble))

    val count = DenseVector.zeros[Double](weight.length)
    for (i <- 0 until 1000000) {
      val k = alias.draw(random)
      count(k) += 1
    }

    val tot_count = sum(count)
    val probs = count.map(_ / tot_count)

    println(count)
    println(probs)
    println(prob)
    val x = probs - prob

    val r = sum(x.map(Math.abs(_)))
    assert ( r < 0.01)
  }

  test("alias table sparse") {
    val random = new FastRNG(1001)
    val default: DenseVector[Double] = DenseVector.fill[Double](100)(random.nextPositiveInt() % 2 + 1)

    val weight = SparseVector.zeros[Double](100)

    for (i <- 0 until 10) {
      val index = random.nextPositiveInt() % 100
      weight(index) = random.nextPositiveInt() %100
    }

    val stats = for (i <- 0 until 100) yield{
      default(i) + weight(i)
    }

    val prob = stats.map(_ / stats.sum)

    val defaultAlias = new AliasTable(default)
    val alias = new AliasTable(weight, defaultAlias)

    val count = DenseVector.zeros[Double](weight.length)
    for (i <- 0 until 1000000) {
      val k = alias.draw(random)
      count(k) += 1
    }

    val tot_count = sum(count)
    val probs = count.map(_ / tot_count).toArray

    println(count)
    println(prob.zipWithIndex.filter(_._1>0.1).mkString(" "))
    println(probs.zipWithIndex.filter(_._1>0.1).mkString(" "))
    val x = for (i <- 0 until 100) yield {probs(i) - prob(i)}

    val r = sum(x.map(Math.abs(_)))
    assert ( r < 0.01)
  }

}
