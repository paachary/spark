//package org.paachary.kafka.scala
//
//import java.util
//import java.util.concurrent.CountDownLatch
//
//import org.apache.kafka.clients.consumer._
//import org.apache.kafka.common.errors.WakeupException
//import org.slf4j.LoggerFactory
//
//import scala.collection.JavaConverters._
//
//class ConsumerWithThread {
//
//  val consumerGroupId = "sixth_application_group"
//
//  def readFromKafka( topic : String) : Unit = {
//
//    val log = LoggerFactory.getLogger(classOf[ConsumerWithThread])
//    val latch : CountDownLatch = new CountDownLatch(1)
//
//    val myConsumerThread  = new ConsumerThread(topic,
//                                               countDownLatch = latch)
//
//    log.info("Creating the consumer")
//    val thread : Thread = new Thread(myConsumerThread)
//    thread.start()
//    Runtime.getRuntime().addShutdownHook(
//      new Thread( () => {
//            log.info("Caught shutdown hook")
//        ( myConsumerThread).shutdown()
//            try {
//                latch.await()
//            } catch  {
//              case exception: InterruptedException => exception.printStackTrace()
//            }
//            log.info("Application has exited");
//        }
//        ))
//
//    try {
//      latch.await()
//    } catch {
//      case exception: Exception => log.error("Application got interrupted : "+ exception)
//    } finally {
//      log.info("Application is closing")
//    }
//  }
//
//  class ConsumerThread(topic: String,
//                       countDownLatch: CountDownLatch) extends Runnable {
//
//    private val log = LoggerFactory.getLogger(classOf[ConsumerThread])
//    private val constants = new Constants
//    private val props = constants.props
//
//    props.put(ConsumerConfig.GROUP_ID_CONFIG , consumerGroupId)
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//
//    private val consumer = new KafkaConsumer[String, String](props)
//
//    consumer.subscribe(util.Arrays.asList(topic)) //multiple topics
//
//    override def run(): Unit = {
//      try {
//        while(true) {
//          val record = consumer.poll(1000).asScala
//          for (data <- record.iterator)
//            log.info(data.key() + ":" +
//              data.value() + ":" +
//              data.offset() + ":" +
//              data.partition())
//        }
//      } catch {
//        case ex: WakeupException => log.info("Received Shutdown Signal")
//      } finally {
//        consumer.close()
//        countDownLatch.countDown()
//      }
//   }
//
//    def shutdown(): Unit = {
//      consumer.wakeup()
//    }
//  }
//}
//
//object ConsumerWithThread extends  App {
//  new ConsumerWithThread().readFromKafka("first_topic")
//}
