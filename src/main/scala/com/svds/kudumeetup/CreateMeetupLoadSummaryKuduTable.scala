package com.svds.kudumeetup

import java.util

import org.kududb.{Schema, Type, ColumnSchema}
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.client.{PartialRow, CreateTableBuilder, KuduClient}

object CreateLoadSummaryKuduTable {
  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("{kuduMaster} {tableName}")
      return
    }

    val kuduMaster = args(0)
    val tableName = args(1)

    val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    val columnList = new util.ArrayList[ColumnSchema]()

    columnList.add(new ColumnSchemaBuilder("time", Type.INT64).key(true).build())
    columnList.add(new ColumnSchemaBuilder("rsvp_cnt", Type.DOUBLE).key(false).build())
    val schema = new Schema(columnList)

    if (kuduClient.tableExists(tableName)) {
      println("Deleting Table")
      kuduClient.deleteTable(tableName)
    }
    val createTableBuilder = new CreateTableBuilder
    println("Creating Table")
    kuduClient.createTable(tableName, schema, createTableBuilder)
    println("Created Table")
    kuduClient.shutdown()
  }
}

