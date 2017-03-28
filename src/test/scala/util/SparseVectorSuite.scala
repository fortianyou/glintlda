package util

import breeze.linalg.SparseVector
import org.scalatest.FunSuite

/**
  * Created by Roger on 17/3/28.
  */
class SparseVectorSuite extends FunSuite{

  test("test sparse vector") {
    val vec = SparseVector.zeros[Int](10)
    vec(0) = 1
    vec(2) = 2
    val newVec = vec.mapActivePairs{
      case (k, v) =>
        v + 1
    }

    newVec(2) = 0
    println(newVec)
    assert(newVec(0) == 2)
  }
}
