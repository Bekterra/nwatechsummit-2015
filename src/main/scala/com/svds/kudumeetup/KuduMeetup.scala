package com.svds.kudumeetup

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.kududb.client.PartialRow
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.spark.KuduContext
import org.kududb.spark.KuduDStreamFunctions.GenericKuduDStreamFunctions

import scala.util.Try

object KuduMeetup {
  def loadDataFromKafka(topics: String,
                        brokerList: String,
                        ssc: StreamingContext): DStream[String] = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.map(_._2)
  }

  def setCol(kuduRow: PartialRow, sparkRow: => Row, getField: String, setField: String = null): Unit = {
    val field = if (setField == null) getField else setField
    Try(sparkRow.get(sparkRow.fieldIndex(getField))).getOrElse(null) match {
      case value: Int =>
        Try(kuduRow.addInt(field, value)) orElse Try(kuduRow.addLong(field, value))
      case value: Long =>
        Try(kuduRow.addLong(field, value)) orElse Try(kuduRow.addInt(field, value.toInt))
      case value: Double =>
        kuduRow.addDouble(field, value)
      case value: String =>
        kuduRow.addString(field, value)
      case _ =>
        Try(kuduRow.setNull(field)) orElse
        Try(kuduRow.addInt(field, 0)) orElse
        Try(kuduRow.addLong(field, 0)) orElse
        Try(kuduRow.addDouble(field, 0)) orElse
        Try(kuduRow.addString(field, ""))
    }
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Required arguments: <kudu host> <kafka host>")
      sys.exit(42)
    }
    val Array(kuduHost, kafkaHost) = args
    val conf = new SparkConf().setAppName("kudu meetup")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(10))
    val kuduContext = new KuduContext(ssc.sparkContext, kuduHost)
    val brokerList = kafkaHost
    val topics = "meetupstream"

    val stream = loadDataFromKafka(topics, brokerList, ssc).transform { rdd =>
      val parsed = sqlContext.read.json(rdd)
      parsed.printSchema()
      parsed.rdd
    }

    stream.kuduForeachPartition(kuduContext, (it, kuduClient, asyncKuduClient) => {
      val table = kuduClient.openTable("kudu_meetup_rsvps")
      //This can be made to be faster
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
      var upserts = 0
      while (it.hasNext) {
        val metadata = it.next()
        val event = metadata.getAs[Row]("event")
        val member = metadata.getAs[Row]("member")
        val other_services = Try(member.getAs[Row]("other_services")).getOrElse(null)
        val venue = metadata.getAs[Row]("venue")

        val operation = table.newInsert()
        val row = operation.getRow
        setCol(row, event, "event_id")
        setCol(row, member, "member_id")
        setCol(row, metadata, "rsvp_id")
        setCol(row, event, "event_name")
        setCol(row, event, "event_url")
        setCol(row, event, "time", "TIME")
        setCol(row, metadata, "guests")
        setCol(row, member, "member_name")
        setCol(row, other_services.getAs[Row]("facebook"), "identifier", "facebook_identifier")
        setCol(row, other_services.getAs[Row]("linkedin"), "identifier", "linkedin_identifier")
        setCol(row, other_services.getAs[Row]("twitter"), "identifier", "twitter_identifier")
        setCol(row, member, "photo")
        setCol(row, metadata, "mtime")
        setCol(row, metadata, "response")
        setCol(row, venue, "lat")
        setCol(row, venue, "lon")
        setCol(row, venue, "venue_id")
        setCol(row, venue, "venue_name")
        setCol(row, metadata, "visibility")
        session.apply(operation)
        upserts += 1
      }
      session.close()
      println("upserts: " + upserts)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

