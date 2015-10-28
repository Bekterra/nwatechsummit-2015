#Kudu Meetup Spark Streaming Example

For the moment kudu-spark (from tmalaska/SparkOnKudu) is included as there are no published artifacts yet.

Build

    sbt package

Usage (with Spark 1.5.1)

```
spark-submit --repositories https://repository.cloudera.com/artifactory/repo/ \
--packages "org.kududb:kudu-client:0.5.0,org.apache.spark:spark-streaming_2.10:1.5.1,"\
"org.apache.spark:spark-streaming-kafka_2.10:1.5.1,org.apache.spark:spark-sql_2.10:1.5.1" \
--class com.svds.kudumeetup.KuduMeetup \
target/scala-2.10/kudu-meetup_2.10-1.0.jar <kudu host> <kafka host>
```