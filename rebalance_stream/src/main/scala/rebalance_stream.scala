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

object TripStreaming {
  def main(args: Array[String]) {

    // Create context with 2 second batch interval
    val conf = new SparkConf().setAppName("trip_streaming").set("spark.cassandra.connection.host", "52.26.135.59")
    val ssc = new StreamingContext(conf, Seconds(2))
       
    // ssc.checkpoint("/user/PuppyPlaydate/spark_streaming")

    val brokers = "ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9092"
    val topics = "trip_data"
    val topicsSet = topics.split(",").toSet

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines and show results
    messages.foreachRDD { rdd =>
        // Get the singleton instance of SQLContext
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val ticksDF = lines.map( x => {
                                  val tokens = x.split(";")
                                  Tick(tokens(0), tokens(2).toDouble, tokens(3).toInt)}).toDF()
        val ticks_per_source_DF = ticksDF.groupBy("source")
                                .agg("price" -> "avg", "volume" -> "sum")
                                .orderBy("source")

        ticks_per_source_DF.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}

case class Row(rid: String, date_time: java.sql.Timestamp, count: Int)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

