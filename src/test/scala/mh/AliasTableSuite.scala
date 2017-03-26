package mh

import breeze.linalg.{DenseVector, sum}
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
    for (i <- 0 until 100000) {
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

}
