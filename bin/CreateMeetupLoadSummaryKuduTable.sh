~/spark-1.5.1-bin-hadoop2.6/bin/spark-submit \
--repositories https://repository.cloudera.com/artifactory/repo/ \
--packages "org.kududb:kudu-client:0.5.0" \
--class com.svds.kudumeetup.CreateMeetupLoadSummaryKuduTable \
target/scala-2.10/kudu-meetup_2.10-1.0.jar \
localhost \
kudu_meetup_rsvps_load_summary

