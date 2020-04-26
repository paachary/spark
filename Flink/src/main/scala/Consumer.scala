import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory

class Consumer {

  def setConsumerProperties(bootStrapServers : String,
                            consumerGroupId : String) : Properties = {
    val props = new Properties()
    //val bootStrapServers: String = "localhost:9092"
    //val consumerGroupId = "first_application_group"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10")

    props
  }

  def init(topic : String,
           bootStrapServers : String,
           consumerGroupId : String) : Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    println("topic = "+ topic+": bootstrapServers = "+bootStrapServers+": consumerGroupID = "+ consumerGroupId)

    val props = setConsumerProperties(bootStrapServers, consumerGroupId)
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(),props)

    val dataStream : DataStream[String] = streamEnv.addSource(consumer)

    dataStream.print()
    streamEnv.execute("Kafka Consumer Job")
    println("completed the job")
  }
  val logger = LoggerFactory.getLogger(Consumer.getClass)
}

object Consumer extends  App{

  val consumer = new Consumer

  println("inside the object")

  consumer.logger.info("inside the main method")

  if (args.length < 1 || args.length > 3){
    println("Wrong number of parameters passed to the class.  \n" +
      "Usage >> \t" +
      "consumer <Kafka Topic Name> [<Kafka Bootstrap Server : Port>] [<Kafka Consumer Group Id>]"
    )
    consumer.logger.error("Wrong number of parameters passed to the class.  \n" +
      "Usage >> \t" +
      "consumer <Kafka Topic Name> [<Kafka Bootstrap Server : Port>] [<Kafka Consumer Group Id>]"
    )
  } else {
    var bootStrapServers : String = ""
    var consumerGroupId : String = ""
    if (args.length == 2) {
      bootStrapServers = args(1)
    } else {
      bootStrapServers = "localhost:9092"
      consumerGroupId = "first_application_group"
    }

    if (args.length == 3) {
      bootStrapServers = args(1)
      consumerGroupId = args(2)
    } else {
      bootStrapServers = "localhost:9092"
      consumerGroupId = "first_application_group"
    }
    consumer.init(topic = args(0),
      bootStrapServers = bootStrapServers,
      consumerGroupId = consumerGroupId)
  }

}

