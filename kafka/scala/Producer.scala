package org.paachary.kafka.scala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Producer {

  def writeToKafka( topic : String) : Unit = {
    val constants = new Constants

    val props = constants.props

    val producer = new KafkaProducer[String, String](props)

    for ( counter <- 1 until 10 ) {
      val record = new ProducerRecord[String, String] (topic, "key",
                                                  "value_" + Integer.toString(counter))
      producer.send(record)
    }
    producer.close()
  }
}

object Producer extends  App {

  val producer = new Producer
  producer.writeToKafka("new_topic")
}
