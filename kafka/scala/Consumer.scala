package org.paachary.kafka.scala

import java.util

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer._
import org.slf4j.{Logger, LoggerFactory}
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.duration.Duration

class Consumer {

  val consumerGroupId = "first_application_group"

  def readFromKafka( topic : String) : Unit = {

    val log = LoggerFactory.getLogger(classOf[Consumer])

    val constants = new Constants
    val props = constants.props

    props.put(
      ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic)) //multiple topics

    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        println(data.key() + ":" +
          data.value() + ":" +
          data.offset() + ":" +
          data.partition())

        log.info(data.key() + ":" +
          data.value() + ":" +
          data.offset() + ":" +
          data.partition())
      }
    }
  }
}

object Consumer extends App {

  val consumer = new Consumer
  consumer.readFromKafka("tweet_topic")
}
