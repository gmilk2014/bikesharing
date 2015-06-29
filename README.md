# BikeAlert
======================================

## A real time bike sharing station monitor system
[www.insight-bikealert.com](http://52.26.203.64:5000/)

BikeAlert is a tool to help the bike sharing program owners track the bike station that is low in bikes and suggest nearby stations that the owners can take bikes from to redistribute bikes across the stations. BikeAlert provide both real-time and historical updates on the most recent trip logs using the following technologies:
- Apache Kafka
- Apache HDFS
- Spark
- Spark Streaming
- Apache Cassandra
- Flask with google maps API, Bootstrap and Ajax

![Map Demo] (images/map.jpg)

# BikeAlert Approach
Puppy Playdate uses synthetically generated trip logs which are processed in the batch and real-time component for historical hourly bike counts along with continual map updates.

![pipeline Demo] (images/pipeline.jpg)

## Data Synethesis
The actual log data from Bay Area Bike Share have many fields including Trip ID,Duration,Start Date,Start Station,Start Terminal,End Date,End Station,End Terminal,Bike #,Subscription Type, and Zip Code. For my project, I only need start/end station ID and start/end Date, so I simply randomly generated a (stationID, timestamp) pair for start/end trip logs. StationID is between 1 to 1000 and timestamp is between 2011/6/17 to 2015/6/17.

## Data Ingestion
(stationID, timestamp) pairs were produced by python scripts using the kafka-python package from https://github.com/mumrah/kafka-python.git. Logs were published to two topics (start_data and end_data) with Spark Streaming as consumers. Historical data were pushed into HDFS from local machine mannually by command `$ hdfs dfs -copyFromLocal <historical_data> <hdfs master dataset directory>`. The historical data in HDFS were served as the source of the truth, so they were immutable and used in batch processing to back up the speed layer.

## Batch Processing
Two batch processes were performed for historical batch views:

1. Count number of bikes by station on a hourly granularity
2. Count number of bikes by station up till all the available data

Batch views were directly written into cassandra with the spark-cassandra connector

sbt libarary dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1"
- "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

A full batch process was made to rebuild the entire batch view.

## Real-time Processing
Streaming process was performed for real-time views:

1. Count number of bikes by station on a 5 second interval

Messages streamed into Spark Streaming with the spark-kafka connector
Real-time views were directly written into cassandra with the spark-cassandra connector

sbt library dependencies:
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1"
- "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
- "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"
- "org.apache.spark" % "spark-streaming_2.10" % "1.3.0" % "provided"
- "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0"
  
## Cassandra Schema
Tables:

1. by_county_rt: table populated by Spark Streaming (real-time) representing message count in the last 5 seconds
2. by_county_msgs: table populated by Spark Streaming (real-time) containing recent messages organized by county and date
3. by_county_month: table populated by Spark (batch) containing the historical message counts on a monthly basis
4. by_county_day: table populated by Spark (batch) containing the historical message counts on a daily basis
```
CREATE TABLE by_county_rt (state varchar, county varchar, count int, PRIMARY KEY ( (state, county) ) );
CREATE TABLE by_county_msgs (state varchar, county varchar, date int, time int, message varchar, PRIMARY KEY ( (state, county), date, time ) );
CREATE TABLE by_county_month (state varchar, county varchar, date int, count, int, PRIMARY KEY ( (state, county), date ) );
CREATE TABLE by_county_day (state varchar, county varchar, date int, count, int, PRIMARY KEY ( (state, county), date ) );
```
Date/Time format: 
- by_county_msgs: 
  - yyyymmdd (ex: 20150209)
  - HHMMSS (ex: 120539)
- by_county_month: yyyymm (ex: 201502)
- by_county_day: yyyymmdd (ex: 20150209)

## API calls
Data in JSON format can be displayed in the browser by calling the following from the root index puppyplaydate.website:

- /update_map/
  - retrieve number of messages in the last 5 seconds for all counties in the US
- /new_messages/county_code/
  - retrieve 5 most recent messages from a county
  - county_code example: puppyplaydate.website/new_messages/us-ca-081/
- /update_chart/interval/county_code/
  - return a time series requested by the time interval and county_code
  - interval options: month or day
  - example: puppyplaydate.website/month/us-ca-081/

## Startup Protocol
1. Kafka server
2. Spark Streaming
3. HDFS Kafka consumer
4. Kafka message producer
