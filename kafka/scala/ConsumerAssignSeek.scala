package org.paachary.kafka.scala

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
//import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class ConsumerAssignSeek {

  def readFromKafka( topic : String) : Unit = {
 //   val log = LoggerFactory.getLogger(classOf[ConsumerAssignSeek])

    val constants = new Constants
    val props = constants.props

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)

    val partitionNo : Int = 1

    val topicPartition : TopicPartition = new TopicPartition(topic, partitionNo)
    consumer.assign(util.Arrays.asList(topicPartition))

    val offset : Long = 15L
    consumer.seek(topicPartition, offset)

    while(true) {
      val record = consumer.poll(1000).asScala
     // for (data <- record.iterator)
   //     log.info(data.key() + ":" +
   //value() + ":" +
   //offset() + ":" +
    //    data.partition())
    }
  }
}

object  ConsumerAssignSeek extends App {
  val consumerAssignSeek = new ConsumerAssignSeek
  consumerAssignSeek.readFromKafka("first_topic")
}