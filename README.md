#Kudu Meetup Spark Streaming Example

For the moment kudu-spark (from [tmalaska/SparkOnKudu](https://github.com/tmalaska/SparkOnKudu)) is included as there are no published artifacts yet.

Follow instructions to setup kudu sandbox or run against your own:
http://getkudu.io/docs/quickstart.html

ssh into VM:
`ssh demo@quickstart.cloudera`

Install git:
`sudo yum install git`

Clone repo:
`git clone https://github.com/silicon-valley-data-science/nwatechsummit-2015.git`

Install sbt:
http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html
From link (may need refreshed):
```
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt
```

Build:
```
cd ~/nwatechsummit-2015
sbt package
```

Download Spark 1.5.1 for Hadoop 2.6+ (http://spark.apache.org/downloads.html)
From link (may need refreshed) and expand into home dir:
```
cd ~
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar xvf spark-1.5.1-bin-hadoop2.6.tgz
```

Create Kudu Tables:
```
cd ~/nwatechsummit-2015/bin
./CreateMeetupKuduTable.sh
./CreateMeetupLoadSummaryKuduTable.sh
./CreateMeetupPredictionKuduTable.sh
```

Setup and start Kafka locally (or use existing install):
http://kafka.apache.org/documentation.html#quickstart

Clone Meetup Stream Kafka Loader:
`git clone https://github.com/silicon-valley-data-science/strataca-2015.git`

Install Maven on sandbox - http://preilly.me/2013/05/10/how-to-install-maven-on-centos/ :
`cd ~/strataca-2015/Building-a-Data-Platform/tailer2kafka/`

Follow instructions in README (excluding kafka setup from above - inline below may need refreshed):
`mvn install`

Start Curl to File:
`nohup bin/run_curl_meetup_stream.sh &`

Start Tail File to Kafka:
`nohup bin/run_tailer2kafka.sh &`

Run Streaming Prediction:
```
cd ~/nwatechsummit-2015/bin
./RunKuduMeetupStreamingPrediction.sh
```

Run Raw data to Kudu:
`./RunKuduMeetup.sh`

Go to http://quickstart.cloudera:8051, click tables tab then click each meetup table (for example  kudu_meetup_rsvps) to get schema for impala table

Run impala-shell:
`impala-shell`

Paste in create tables from above thenquery kudu meetup table:
```
select * from kudu_meetup_rsvps limit 2;
select * from kudu_meetup_rsvps_predictions limit 2;
select * from kudu_meetup_rsvps_load_summary limit 2;
```
