import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparator}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job


  /**
   * The mapper class extending the base MapReduceBase and extends the Mapper
   */
  class MovieMapperPopularity extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {

    private final val one = new IntWritable(1)
    private val word = new Text()

    /** *
     * The mapper functionality
     *
     * @param key      -> Key if this is a chained to a previous reduce process
     * @param value    -> The value which we want to act upon
     * @param output   -> We are not acting on it for this program
     * @param reporter -> We are not acting on it for this program.
     */
    override def map(key: LongWritable,
                     value: Text,
                     output: OutputCollector[Text, IntWritable],
                     reporter: Reporter): Unit = {
      // value is the actual input to the mapper, meaning it every line of ratings.
      // We are grouping by ratings and finally aggregating it in the reducer.
      // That would give us the count of each rating value.
      word.set(value.toString.split(",")(1))
      output.collect(word, one)
    }
  }

  /**
   * The reducer class extending the base MapReduceBase and extends the Mapper
   */
  class MovieReducerPopularity extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    /**
     * The reducer program
     *
     * @param key      => The key supplied by the mapper program
     * @param values   => The value provided by the mapper program
     * @param output   => N/A
     * @param reporter => N/A
     */
    override def reduce(key: Text,
                        values: util.Iterator[IntWritable],
                        output: OutputCollector[Text, IntWritable],
                        reporter: Reporter): Unit = {
      import scala.collection.JavaConversions._
      val sum = values.toList.reduce((valueOne, valueTwo) =>
        new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get))
    }
  }

  class IntComparator extends WritableComparator {

    override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
      val v1: Int = ByteBuffer.wrap(b1, s1, l1).getInt
      val v2: Int = ByteBuffer.wrap(b2, s2, l2).getInt

      v1.compareTo(v2) * (-1)
    }
  }

  /**
   * The invoking object
   */
  object MovieRankingByPopularityMapReduce extends App {
    val conf = new JobConf(this.getClass)

    conf.setJobName("MovieRankingByPopularity")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[MovieMapperPopularity])
    conf.setReducerClass(classOf[MovieReducerPopularity])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    //conf.setOutputKeyComparatorClass(classOf[IntComparator])

    FileInputFormat.setInputPaths(conf, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/" +
      "hadoop/data/ratings.csv"))

    FileOutputFormat.setOutputPath(conf, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user" +
      "/hadoop/data/output"))

    JobClient.runJob(conf).waitForCompletion()
  }

