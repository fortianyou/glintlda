package dataserver
import java.util.concurrent.{Executor, Executors}

import you.dataserver.DataServerClient

import scala.util.Random

/**
  * Created by Roger on 17/3/29.
  */
object MultiThreadDataServerTest {

  class DataServerRunnable extends Runnable {
    override def run(): Unit = {
      val ds = new DataServerClient[Int]()


      while(true) {
        for (i <- 0 until 10) {
          val j = Math.abs(Random.nextInt()) % 10
          val k = Math.abs(Random.nextInt()) % 10
          ds.increaseBufferred(i, j, 1)
          ds.increaseBufferred(i, k, -1)
        }
        Thread.sleep(Math.abs(Random.nextInt()) % 1000)
        ds.flushIncBuffer()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    val pool = Executors.newFixedThreadPool(10)

    for (i <- 0 until 10 ) {
      pool.execute(new DataServerRunnable())
    }

    val ds = new DataServerClient[Int]()
    ds.clear()
    println("Start ....")
    while( true ) {
      Thread.sleep(1000)
      println("Report ...")
      val values = ds.pull(1)
      println(values.map(_._2).sum)
    }
  }
}
