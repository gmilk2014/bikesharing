spark-submit --class trip_data --master spark://ip-172-31-18-35:7077 --jars target/scala-2.10/trip_batch_2.10-1.0.jar target/scala-2.10/trip_batch-assembly-1.0.jar --executor-memory 1024m --driver-memory 1024m

