import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.driver.core.utils._
import scala.collection.JavaConversions._
import java.text.DateFormat
import java.text.DateFormat._

object RebalanceStreaming {
  def main(args: Array[String]) {

    // Create context with 5 seconds batch interval
    val conf = new SparkConf().setAppName("trip_streaming").set("spark.cassandra.connection.host", "52.26.135.59")
    val ssc = new StreamingContext(conf, Seconds(5))
       
    ssc.checkpoint("hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/checkpoint")

    // setup broker ip and the topics to subscribe
    val brokers = "ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9092"
    val topics1 = "trip_start_data"
    val topicsSet1 = topics1.split(",").toSet
    val topics2 = "trip_end_data"
    val topicsSet2 = topics2.split(",").toSet

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val start_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet1)
    val end_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet2)
    
    // function to convert a timestamp to a 1 hour time slot
    def convert_to_hourbucket(timestamp: String): String = {

        val record = timestamp.split(" |:")
        record(0) + " " + record(1)
    }

    // set up update function to be the sum of current count + previous count
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val newUpdateFunction = (iterator: Iterator[((Int, java.util.Date), Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    // map each record into a tuple consisting of (stationID, timestamp)
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy HH")

    val start_ticks = start_messages.map(line => {
                         val record = line._2.split(",").map(_.trim)
                        (record(0).toInt,
                         format.parse(convert_to_hourbucket(record(1))))
                         })

    val end_ticks = end_messages.map(line => {
                         val record = line._2.split(",").map(_.trim)
                        (record(0).toInt,
                         format.parse(convert_to_hourbucket(record(1))))
                         })

    // count for each hour bucket
    val start_hour_bucket_count = start_ticks.map(record => (record, 1))

    val end_hour_bucket_count = end_ticks.map(record => (record, -1))

    val hour_bucket_count = start_hour_bucket_count.union(end_hour_bucket_count)
                            .updateStateByKey[Int](newUpdateFunction,
      new HashPartitioner (ssc.sparkContext.defaultParallelism), true)
      .map(record => (record._1._1, record._1._2, record._2))

    val bike_count_by_station = hour_bucket_count.map(record => (record._1, record._3)).reduceByKey(_+_).map(record => (((record._1 - 1) / 100 + 1), record._1, record._2))

    // save in Cassandra
    hour_bucket_count.saveToCassandra("bikeshare", "rebalance_stream")
    bike_count_by_station.saveToCassandra("bikeshare", "bikecount_stream")
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
