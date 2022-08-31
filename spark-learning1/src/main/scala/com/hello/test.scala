package com.hello

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object test {

  def main(args: Array[String]): Unit = {
    val f_array : Array[Future[Int]] = Array(
      Future{
        Thread.sleep(200)
        1
      },
      Future{
        Thread.sleep(1000)
        2
      },
      Future{
        Thread.sleep(150)
        3
      }
    )
    f_array.foreach(f => f.onSuccess{
      case c => println(c)
    })
    Thread.sleep(10000)
  }
}