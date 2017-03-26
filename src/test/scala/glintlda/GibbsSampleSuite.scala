package glintlda

import breeze.linalg.SparseVector
import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by Roger on 17/3/24.
  */
class GibbsSampleSuite extends FunSuite{

  test("binary search cutoff in sparse vector") {
    val sv = SparseVector.zeros[Int](10000)
    val cutoff =  4999

    sv(1) = 1
    var findIndex: Int = GibbsSample.binarySearch(sv, cutoff)
    println(sv.index.slice(0, sv.activeSize).mkString(" "))
    assert(findIndex == sv.activeSize)


    sv(9999) = 1

    findIndex = GibbsSample.binarySearch(sv, cutoff)
    assert(findIndex < sv.activeSize && sv.indexAt(findIndex) == 9999)

    sv(5001) = 5

    findIndex = GibbsSample.binarySearch(sv, cutoff)
    assert(findIndex < sv.activeSize && sv.indexAt(findIndex) == 5001)


    for (i <- 0 until 100) {
      val index = Math.abs(Random.nextInt()) % 10000
      sv(index) = 1
    }

    sv(4999) = 1
    findIndex = GibbsSample.binarySearch(sv, cutoff)
    assert(findIndex < sv.activeSize && sv.indexAt(findIndex) == 4999)

    assert(sv.index.size < 200)
    assert(sv.size == 10000)
    println(sv.index.slice(0, sv.activeSize).mkString(" "))
  }
}
