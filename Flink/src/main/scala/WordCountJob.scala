import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.runtime.fs.hdfs.HadoopRecoverableWriter

object WordCountJob {
//  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    /*
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")
     */

    val text = env.readTextFile("/home/hadoop/textFile.txt")
    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // emit result
    counts.writeAsText("/home/hadoop/wordcount-flink-output.txt",writeMode = FileSystem.WriteMode.OVERWRITE)

    env.execute()
//  }
}
