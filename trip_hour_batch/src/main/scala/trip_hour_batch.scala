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
    val folder_name = "hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/masterdata/trip/"

    // function to convert a timestamp to a 1 hour time slot
    def convert_to_hourbucket(timestamp: String): String = {
        
        val record = timestamp.split(" |:")
        record(0) + " " + record(1) + " PST"
    }

    // read in the data from HDFS
    val file = sc.textFile(folder_name)

    // map each record into a tuple consisting of
    // (Trip ID, Duration, Start Date, Start Station, Start Terminal,
    // End Date,End Station,End Terminal, Bike #, Subscription Type, Zip Code)
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy HH zzz")
    val ticks = file.map(line => {
                         val padded_line = line + " "
                         val record = padded_line.split(",").map(_.trim)
                        (record(0).toInt, record(1).toInt,
                         format.parse(convert_to_hourbucket(record(2))),
                         record(3), record(4).toInt, record(5), record(6),
                         record(7).toInt, record(8).toInt, record(9),
                         record(10))
                                 }).persist

    // apply the time conversion to the time portion of each tuple and persist it memory for later use

    // count for each hour bucket
    val hour_bucket_count = ticks.map(record => (record._3, 1))
                                 .reduceByKey(_+_).sortByKey()

    // assign record id
    // var id = 0
    val hour_bucket_count_id = hour_bucket_count.map(record => {
                        (1, record._1, record._2)})

    // save the data back into HDFS
    // hour_bucket_count.saveAsTextFile("hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/output/trip_data_output_scala")

    // save in Cassandra
    hour_bucket_count_id.saveToCassandra("bikeshare", "test_by_hour_batch") 
  }
}
