package main.scala

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.io.{NullWritable, IntWritable, Text, LongWritable}
import org.apache.hadoop.mapred.{OutputCollector, Reporter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import org.apache.hadoop.util.{ToolRunner, Tool}
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import java.lang
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * Simple wordcount example with HBASE integration.
 * This simple example could be used to test if the Hadoop 2.2 and Hbase 0.98 configuration is up and working.
 *
 * Created by marbarfa on 3/17/14.
 */
object WordCount extends Configured with Tool {

  def main(args: Array[String]) {
    var res = ToolRunner.run(new Configuration(), WordCount, args);
    System.exit(res);
  }

  def run(args: Array[String]): Int = {
    var input = args(0);
    var output = args(1)


    //creating a JobConf object and assigning a job name for identification purposes
    val job: Job = Job.getInstance(getConf, WordCount.getClass.getSimpleName)


    job.setJobName("WordCount")

    //Setting configuration object with the Data Type of output Key and Value
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    //Providing the mapper and reducer class names
    job.setMapperClass(classOf[WordCountMapper])
    job.setReducerClass(classOf[WordCountReducer])

    //the hdfs input and output directory to be fetched from the command line
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))

    var finishedOK = job.waitForCompletion(true)
    if (finishedOK) {
      return 0;
    }

    return 1
  }


  class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {


    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {
      //map each work to "word" => "1"
      value
        .toString
        .split(" ")
        .map(x => context.write(new Text(x), new IntWritable(1)))
    }

  }

  class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    var table: HTable = _


    protected override def setup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {
      // Get HBase table of invalid variable combination
      val hconf = HBaseConfiguration.create
      table = new HTable(hconf, "wordcount_table")
    }

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {
      var sum = values.asScala
        .foldLeft(0) {(sum, n) => sum + n.get()}
      var put = new Put(("row"+key.toString).getBytes)
      put.add("cf".getBytes, "a".getBytes, sum.toString.getBytes);
      table.put(put);
      context.write(key, new IntWritable(sum))
    }
  }


}
