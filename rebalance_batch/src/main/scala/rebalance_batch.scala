import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import java.text.DateFormat
import java.text.DateFormat._

object trip_data {
  def main(args: Array[String]) {

    // setup the Spark Context named sc
    val conf = new SparkConf().setAppName("countTripByHour").set("spark.cassandra.connection.host", "52.26.135.59")
    val sc = new SparkContext(conf)

    // folder on HDFS to pull the data from
    val start_folder_name = "hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/masterdata/start"

    val end_folder_name = "hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/masterdata/end"

// function to convert a timestamp to a 1 hour time slot
    def convert_to_hourbucket(timestamp: String): String = {

        val record = timestamp.split(" |:")
        record(0) + " " + record(1)
    }

    // read in the data from HDFS
    val start_file = sc.textFile(start_folder_name)
    val end_file = sc.textFile(end_folder_name)

    // map each record into a tuple consisting of ()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy HH")
    val start_ticks = start_file.map(line => {
                         val record = line.split(",").map(_.trim)
                        (record(0).toInt,
                         format.parse(convert_to_hourbucket(record(1))))
                         })

    val end_ticks = end_file.map(line => {
                         val record = line.split(",").map(_.trim)
                        (record(0).toInt,
                         format.parse(convert_to_hourbucket(record(1))))
                         })

// count for each hour bucket
    val start_hour_bucket_count = start_ticks.map(record => (record, 1))

    val end_hour_bucket_count = end_ticks.map(record => (record, -1))

    val hour_bucket_count = start_hour_bucket_count.union(end_hour_bucket_count)
                                 .reduceByKey(_+_).sortByKey().map(
                                  record => (record._1._1, record._1._2, record._2))

    // save the data back into HDFS
    // hour_bucket_count.saveAsTextFile("hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/output/trip_data_output_scala")

    // save in Cassandra
    hour_bucket_count.saveToCassandra("bikeshare", "rebalance_batch")
  }
}

