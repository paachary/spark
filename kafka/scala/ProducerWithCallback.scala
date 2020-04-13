package org.paachary.kafka.scala

import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
//import org.slf4j.LoggerFactory

class ProducerWithCallback {

  def writeToKafka( topic : String) : Unit = {

 //   val log = LoggerFactory.getLogger(ProducerWithCallback.getClass())

    val constants = new Constants

    val props = constants.props

    val producer = new KafkaProducer[String, String](props)
    for ( counter <- 1 until 10 ) {
     val key = "-id_"+Integer.toString(counter)
      val value = "value_"+Integer.toString(counter)
      val record = new ProducerRecord[String, String](topic, key, value)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =  {
          if (exception == null)
              println( "Received Metadata. \n" +
  //          log.info("Received Metadata. \n" +
              " Topic = "+metadata.topic() + ":" +
              " key = " + key + ":" +
              " value = " + value + ":" +
              " Partition = "+ metadata.partition() + ":" +
              " Offset = " + metadata.offset() +":" +
              " Timestamp = " + metadata.timestamp() +"\n")
          else
            println("Exception occurred = " + exception)
           //// log.error("Exception occurred = " + exception)
        }
      })
    }
    producer.close()
  }
}

object ProducerWithCallback extends  App {
  val producerWithCallback = new ProducerWithCallback
  producerWithCallback.writeToKafka("first_topic")
}


