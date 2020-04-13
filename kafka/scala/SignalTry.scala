package org.paachary.kafka.scala

import sun.misc.Signal
import sun.misc.SignalHandler

object SignalTry extends  App{

    val start = System.nanoTime()
    var counter = 0

    Signal.handle(new Signal("INT"), new SignalHandler() {
      def handle(sig: Signal) {
        println(f"\nProgram execution took ${(System.nanoTime() - start) / 1e9f}%f seconds\n")
        System.exit(0)
      }
    })

    while (true) {
      counter += 1
      println(counter)
      Thread.sleep(500)
    }
}
