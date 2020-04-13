package org.paachary.kafka.scala

import java.io.{File, FileNotFoundException, PrintWriter}

import twitter4j._
//`import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

class TwitterParsing {

  def beginParsing : Unit = {

   // val logger = LoggerFactory.getLogger(TwitterParsing.getClass.getName)
    val twitterStream = new TwitterStreamFactory(Util.config).getInstance()
    twitterStream.addListener(Util.simpleStatusListener)
    try {
      Signal.handle(new Signal("INT"), new SignalHandler() {
      def handle(sig: Signal) {
     //   logger.info("Process interrupted. Gracefully exiting...")
        twitterStream.cleanUp()
        twitterStream.shutdown()
        System.exit(0)
      }
    })
      var fileCounter = 0
      while (true) {
        fileCounter += 1
        Util.init("json/tweeter"+fileCounter+".json")
       // logger.info("inside the while loop")
        twitterStream.sample()
        Thread.sleep(1000)
        Util.writer.close()
      }
    } catch {
      case exception: Exception =>    println("Exception =" + exception) //logger.error("Exception ="+exception)
    } finally {
//      logger.info("Inside finally block")
      twitterStream.cleanUp()
      twitterStream.shutdown()
    }
  }
}

object Util {

  var file : File = new File("sample.txt")
  var writer : PrintWriter = new PrintWriter(file)

  def init (fileName : String) : Unit = {
    file = new File(fileName)
    writer = new PrintWriter(fileName)
  }

 // val logger = LoggerFactory.getLogger(TwitterParsing.getClass)

  parseTwitterCred

 // logger.debug(System.getProperty("twitter4j.oauth.consumerKey") +"\n"+
 //   System.getProperty("twitter4j.oauth.consumerSecret") +"\n"+
 //   System.getProperty("twitter4j.oauth.accessToken") + "\n" +
  //  System.getProperty("twitter4j.oauth.accessTokenSecret")
 // )

  val config = new twitter4j.conf.ConfigurationBuilder().
   setOAuthConsumerKey(System.getProperty("twitter4j.oauth.consumerKey")).
   setOAuthConsumerSecret(System.getProperty("twitter4j.oauth.consumerSecret")).
   setOAuthAccessToken(System.getProperty("twitter4j.oauth.accessToken")).
   setOAuthAccessTokenSecret(System.getProperty("twitter4j.oauth.accessTokenSecret")).
   build()

  def simpleStatusListener = new StatusListener {
      override def onStatus(status: Status): Unit =
      {
        Util.writer.write(
          "{ \"Id\":"+status.getId+","+
            " \"Timestamp\":\""+status.getCreatedAt+"\","+
            "\"UserInfo\":\""+ status.getUser+"\","+
      //      "\"Text\":"+ status.getText+"," +
            " \"FavoriteCount\":"+ status.getFavoriteCount +"} \n"
        )
     /*   logger.debug(
        "{ \"Id\":\""+status.getId+"\",\n"+
        " \"Timestamp\":\""+status.getCreatedAt+"\",\n"+
        "\"UserInfo\":\""+ status.getUser+"\",\n"+
        "\"Text\":\""+ status.getText+"\",\n" +
        " \"FavoriteCount\":\""+ status.getFavoriteCount +"\"} \n"
        )

      */
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

      override def onStallWarning(warning: StallWarning): Unit = {}

      override def onException(ex: Exception): Unit = {}
    }

  /** Configures Twitter service credentials using twiter.txt
   * in the main workspace directory */

  def parseTwitterCred(): Unit = {
    import scala.io.Source

    try {
      for (line <- Source.fromFile( "twitter.txt"). getLines) {
        val fields = line.split(" ")
        if (fields.length == 2) {
          System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        }
      }
    } catch {
      case exception: FileNotFoundException =>
        println("Error while opening the file")
        //logger.error("Error while opening the file")
    }
  }
}

object TwitterParsing extends App {
  val twitterParsing = new TwitterParsing
  twitterParsing.beginParsing
}