package you.dataserver

import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by Roger on 17/3/24.
  */
class DataServerClientSuite extends FunSuite{

  test("pustToBuffer, flush, pull") {
    val client = new DataServerClient[Double]()
    for (i <- 0 until 1050) {
      while (!client.pushToBuffer(i, i, i * 0.5))
        client.flushBuffer()
    }

    client.flushBuffer()

    val values = client.pull((0 until 1050), (0 until 1050))

    for (i <- 0 until 1050) {
      assert( math.abs(values(i) - (i*0.5)) < Double.MinPositiveValue)
    }

    client.destroy()
  }

  test("incBufferred, delBufferred") {
    val client = new DataServerClient[Int]()
    for (i <- 0 until 1050) {
      while (!client.increaseBufferred(i, i, i))
        client.flushIncBuffer()
    }

    for (i <- 0 until 1050) {
      while (!client.increaseBufferred(i, i, -i))
        client.flushIncBuffer()
    }
    client.flushIncBuffer()

    assert( 0 == client.pull((0 until 1050), (0 until 1050)).sum )

    for (i <- 0 until 1050) {
      while (!client.delBufferred(i, i))
        client.flushDelBuffer()
    }
    client.flushDelBuffer()

    var count = 0
    for (i <- 0 until 1050) {
      val res = client.pull(Array(i), Array(i))
      count += res.sum
    }

    assert( count == 0)


    client.destroy()
  }
}
