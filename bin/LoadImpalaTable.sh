~/spark-1.5.1-bin-hadoop2.6/bin/spark-submit \
--repositories https://repository.cloudera.com/artifactory/repo/ \
--packages "org.kududb:kudu-client:0.5.0,org.apache.spark:spark-streaming_2.10:1.5.1,"\
"org.apache.spark:spark-streaming-kafka_2.10:1.5.1,org.apache.spark:spark-sql_2.10:1.5.1" \
--class com.svds.kudumeetup.KuduMeetupParquetLoad \
../target/scala-2.10/kudu-meetup_2.10-1.0.jar \
/user/ubuntu/meetup_parquet \
file:/mnt/meetupstream/meetupstream.json
