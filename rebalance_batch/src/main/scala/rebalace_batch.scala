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
        record(0) + " " + record(1) + " PST"
    }

    // read in the data from HDFS
    val start_file = sc.textFile(start_folder_name)
    val end_file = sc.textFile(end_folder_name)

    // map each record into a tuple consisting of
    // (Trip ID, Duration, Start Date, Start Station, Start Terminal,
    // End Date,End Station,End Terminal, Bike #, Subscription Type, Zip Code)
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy HH zzz")
    val ticks = file.map(line => {
                         val record = line.split(",").map(_.trim)
                        (record(0).toInt,
                         format.parse(convert_to_hourbucket(record(1)))
                         }).persist

    // count for each hour bucket
    val hour_bucket_count = ticks.map(record => (record._1, 1))
                                 .reduceByKey(_+_).sortByKey()

    // save the data back into HDFS
    // hour_bucket_count.saveAsTextFile("hdfs://ec2-52-26-135-59.us-west-2.compute.amazonaws.com:9000/output/trip_data_output_scala")

    // save in Cassandra
    hour_bucket_count_id.saveToCassandra("bikeshare", "test_by_hour_batch") 
  }
}
