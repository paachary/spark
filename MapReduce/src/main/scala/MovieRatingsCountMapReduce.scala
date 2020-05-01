import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf,
  MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

/**
 *  The mapper class extending the base MapReduceBase and extends the Mapper
 */
class MovieMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable ] {

  private final val one = new IntWritable(1)
  private val word = new Text()

  /***
   * The mapper functionality
   * @param key -> Key if this is a chained to a previous reduce process
   * @param value -> The value which we want to act upon
   * @param output -> We are not acting on it for this program
   * @param reporter -> We are not acting on it for this program.
   */
  override def map(key: LongWritable,
                   value: Text,
                   output: OutputCollector[Text, IntWritable],
                   reporter: Reporter): Unit = {
    // value is the actual input to the mapper, meaning it every line of ratings.
    // We are grouping by ratings and finally aggregating it in the reducer.
    // That would give us the count of each rating value.
    word.set(value.toString.split(",")(2))
    output.collect(word,one)
  }
}
/**
 *  The reducer class extending the base MapReduceBase and extends the Mapper
 */
class MovieReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
  /**
   * The reducer program
   * @param key => The key supplied by the mapper program
   * @param values => The value provided by the mapper program
   * @param output => N/A
   * @param reporter => N/A
   */
  override def reduce(key: Text,
                      values: util.Iterator[IntWritable],
                      output: OutputCollector[Text, IntWritable],
                      reporter: Reporter): Unit =
  {
    import scala.collection.JavaConversions._
    val sum = values.toList.reduce( (valueOne , valueTwo ) =>
      new IntWritable(valueOne.get()+ valueTwo.get()))
    output.collect(key, new IntWritable(sum.get))
  }
}

/**
 * The invoking object
 */
//object MovieRatingsCountMapReduce extends App {
object MovieRatingsCountMapReduce {
  val conf = new JobConf(this.getClass)
  conf.setJobName("MovieWithRatingCount")
  conf.setOutputKeyClass(classOf[Text])
  conf.setOutputValueClass(classOf[IntWritable])
  conf.setMapperClass(classOf[MovieMapper])
  conf.setReducerClass(classOf[MovieReducer])
  conf.setInputFormat(classOf[TextInputFormat])
  conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

  FileInputFormat.setInputPaths(conf, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/" +
    "hadoop/data/ratings.csv"))

  FileOutputFormat.setOutputPath(conf, new Path( "hdfs://sandbox-hdp.hortonworks.com:8020/user" +
    "/hadoop/data/output"))

  JobClient.runJob(conf)
}
