package com.svds.kudumeetup

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.kududb.client.PartialRow
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.KuduContext
import org.kududb.spark.KuduDStreamFunctions.GenericKuduDStreamFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.Try

object KuduMeetupStreamingPrediction {
  def loadDataFromKafka(topics: String,
                        brokerList: String,
                        ssc: StreamingContext): DStream[String] = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.map(_._2)
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    if (args.length != 2) {
      println("Required arguments: <kudu host> <kafka host>")
      sys.exit(42)
    }
    val Array(kuduHost, kafkaHost) = args
    val conf = new SparkConf().setAppName("kudu meetup")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(600))
    val kuduContext = new KuduContext(ssc.sparkContext, kuduHost)
    val brokerList = kafkaHost
    val topics = "meetupstream"
    val current_time = System.currentTimeMillis

    val numFeatures = 2
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))

    //val windowed_dstream = loadDataFromKafka(topics, brokerList, ssc).window(new Duration(60000), new Duration(60000))
    val dstream = loadDataFromKafka(topics, brokerList, ssc)

    //val stream = windowed_dstream.transform { rdd =>
    val stream = dstream.transform { rdd =>
      val parsed1 = sqlContext.read.json(rdd)
      parsed1.registerTempTable("parsed1")
      val parsed2 = sqlContext.sql("select m,cnt,mtime from (select (int(mtime/60000)-(" + current_time + "/60000 ))/1000.0 as m,count(*) as cnt,int(mtime/60000) as mtime from (select distinct * from parsed1) aa group by (int(mtime/60000)-(" + current_time + "/60000 ))/1000.0,int(mtime/60000) ) aa where cnt > 20 ")
      parsed2.rdd
    }
    stream.print()

    val actl_stream = stream.map(x => LabeledPoint(x(1).toString.toDouble, Vectors.dense(Array(1.0,x(0).toString.toDouble))) ).cache()
    actl_stream.print()
    val pred_stream = stream.map(x => LabeledPoint((x(2).toString.toDouble+10)*60000, Vectors.dense(Array(1.0,x(0).toString.toDouble))) )
    pred_stream.print()

    model.trainOn(actl_stream)
    val rslt_stream = model.predictOnValues(pred_stream.map(lp => (lp.label, lp.features)))
    rslt_stream.print()

    stream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
      val table = kuduClient.openTable("kudu_meetup_rsvps_load_summary")
      //This can be made to be faster
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
      var upserts = 0
      while (it.hasNext) {
        val metadata = it.next()

        val operation = table.newInsert()
        val row = operation.getRow
        row.addLong("time", metadata(2).toString.toDouble.toLong*60000)
        row.addDouble("rsvp_cnt", metadata(1).toString.toDouble)
        session.apply(operation)
        upserts += 1
      }
      session.close()
    })
    rslt_stream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
      val table = kuduClient.openTable("kudu_meetup_rsvps_predictions")
      //This can be made to be faster
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
      var upserts = 0
      while (it.hasNext) {
        val metadata = it.next()

        val operation = table.newInsert()
        val row = operation.getRow
        row.addLong("time", metadata._1.toString.toDouble.toLong)
        row.addDouble("rsvp_cnt", metadata._2.toString.toDouble)
        session.apply(operation)
        upserts += 1
      }
      session.close()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//KuduMeetupStreamingPrediction.main(Array("localhost","localhost:9092"))
