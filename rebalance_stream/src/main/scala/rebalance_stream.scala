import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.driver.core.utils._
import scala.collection.JavaConversions._
import java.text.DateFormat
import java.text.DateFormat._

object RebalanceStreaming {
  def main(args: Array[String]) {

    // Create context with 1 second batch interval
    val conf = new SparkConf().setAppName("trip_streaming").set("spark.cassandra.connection.host", "52.26.135.59")
    val ssc = new StreamingContext(conf, Seconds(1))
       
    ssc.checkpoint("hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/checkpoint")

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

    // map each record into a tuple consisting of ()
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
                                 .reduceByKey(_+_).map(
                                  record => (record._1._1, record._1._2, record._2)) 
    // save in Cassandra
    hour_bucket_count.saveToCassandra("bikeshare", "rebalance_stream")
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
