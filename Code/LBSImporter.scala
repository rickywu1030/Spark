package idv.rickywu.spark.streaming

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by rickywu on 2015/2/12.
 */
object LBSImporterTest {
  case class LBS3G(datetime: String, msisdn: String, cellid: String, lacod: String)
  case class FILTER_CONF(cellid: String)

  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaTop10Url <zkQuorum> <group> <topics> <numThreads>")
//      System.err.println("Sample: KafkaTop10Url 172.xxx.xxx.xxx:2181 console-consumer-79829 flumeData 5")
//      System.exit(1)
//    }
    val sparkConf = new SparkConf().setAppName("LBSImporterTest")
    val sc = new SparkContext(sparkConf)
    val interval = 10 // seconds
    val ssc = new StreamingContext(sc, Seconds(interval))

    val lines = ssc.textFileStream("LBS/input")
    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD
    lines.foreachRDD(rdd=>{
      var out = ArrayBuffer[String]()
      if (rdd.count > 0) {
        // LBS 3G Data
        val lbs3gData = rdd.map(_.split(",")).map(d => LBS3G(d(0), d(1), d(3), d(6)))
        lbs3gData.registerTempTable("LBS3G")
        // Filter config
        val filterConf = sc.textFile("LBS/conf/filter.conf").map(_.split(",")).map(d => FILTER_CONF(d(0)))
        filterConf.registerTempTable("FILTER_CONF")
        val joinedQuery = sqlContext.sql("SELECT a.datetime, a.msisdn, a.cellid, a.lacod FROM LBS3G a JOIN FILTER_CONF b ON a.cellid = b.cellid")
        //joinedQuery.foreach(println)
        //joinedQuery.collect().foreach(println)
//        val result = joinedQuery.collect()
//        result.foreach(println)
//        for(iterator <- result.toArray) {
        for(iterator <- joinedQuery.collect().toArray) {
          val datetime = iterator.getString(0)
          val msisdn = iterator.getString(1)
          val cellid = iterator.getString(2)
          val lacod = iterator.getString(3)
          out += datetime + "," + msisdn + "," + cellid + "," + lacod
        }
        println(s"output to HDFS:$out")
      }
      if (out.size>0) {
        val sdf = new java.text.SimpleDateFormat("yyyyMMddHHmm")
        val current = sdf.format(java.util.Calendar.getInstance().getTime)
        val outData = sc.parallelize(out)
        outData.repartition(1).saveAsTextFile("LBS/output/result_" + current)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

object LBSImporterToHbase {
  case class LBS3G(datetime: String, msisdn: String, cellid: String, lacod: String)

  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaTop10Url <zkQuorum> <group> <topics> <numThreads>")
    //      System.err.println("Sample: KafkaTop10Url 172.xxx.xxx.xxx:2181 console-consumer-79829 flumeData 5")
    //      System.exit(1)
    //    }
    val sparkConf = new SparkConf().setAppName("LBSImporterToHbase")
    val sc = new SparkContext(sparkConf)
    val interval = 10 // seconds
    val ssc = new StreamingContext(sc, Seconds(interval))

    val lines = ssc.textFileStream("LBS/input")
    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD
    lines.foreachRDD(foreachFunc = rdd => {
      var out = ArrayBuffer[String]()
      if (rdd.count > 0) {
        // LBS 3G Data
        val lbs3gData = rdd.map(_.split(",")).map(d => LBS3G(d(0), d(1), d(3), d(6)))
        lbs3gData.registerTempTable("LBS3G")
        // Filter config
        val query = sqlContext.sql("SELECT a.datetime, a.msisdn, a.cellid, a.lacod FROM LBS3G a where a.cellid != 0")
        //joinedQuery.foreach(println)
        //joinedQuery.collect().foreach(println)
        //        val result = joinedQuery.collect()
        //        result.foreach(println)
        //        for(iterator <- result.toArray) {
        for (iterator <- query.collect().toArray) {
          val datetime = iterator.getString(0)
          val msisdn = iterator.getString(1)
          val cellid = iterator.getString(2)
          val lacod = iterator.getString(3)
          out += datetime + "," + msisdn + "," + cellid + "," + lacod
        }
        println(s"output to HDFS:$out")

        // Write to HBase
        val tableName = "LBS_3G"

        val conf = HBaseConfiguration.create()
        // Add local HBase conf
        //conf.addResource(new Path("file:///opt/mapr/hbase/hbase-0.94.17/conf/hbase-site.xml"))
        conf.addResource(new Path("file:///opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml"))
        conf.set(TableInputFormat.INPUT_TABLE, tableName)

        // create table with column family
        val admin = new HBaseAdmin(conf)
        if (!admin.isTableAvailable(tableName)) {
          print("Creating LBS_3G_DATA Table")
          val tableDesc = new HTableDescriptor(tableName)
          tableDesc.addFamily(new HColumnDescriptor("cf1".getBytes))
          admin.createTable(tableDesc)
        } else {
          print("Table already exists!!")
          val columnDesc = new HColumnDescriptor("cf1")
          admin.disableTable(Bytes.toBytes(tableName))
          admin.addColumn(tableName, columnDesc)
          admin.enableTable(Bytes.toBytes(tableName))
        }

        //put data into table
        val myTable = new HTable(conf, tableName)
        for (i <- 0 to 5) {
          var p = new Put(new String("row" + i).getBytes)
          p.add("cf1".getBytes, "column-1".getBytes, new String(
            "value " + i).getBytes)
          myTable.put(p)
        }
        myTable.flushCommits()

      }
      if (out.size > 0) {
        val sdf = new java.text.SimpleDateFormat("yyyyMMddHHmm")
        val current = sdf.format(java.util.Calendar.getInstance().getTime)
        val outData = sc.parallelize(out)
        outData.repartition(1).saveAsTextFile("LBS/output/result_" + current)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

object HbaseTest {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: HbaseTest <table> <rowkey> <cf> <column> <value>")
      System.err.println("Sample: HbaseTest LBS_3G 886922447001 info CELLID 12345")
      System.exit(1)
    }
    val Array(table, rowkey, cf, column, value) = args
//    val sparkConf = new SparkConf().setAppName("HbaseTest")
//    val sc = new SparkContext(sparkConf)

    // Write to HBase
    val tableName = table

    val conf = HBaseConfiguration.create()
    // Add local HBase conf
    //conf.addResource(new Path("file:///opt/mapr/hbase/hbase-0.94.17/conf/hbase-site.xml"))
    conf.addResource(new Path("file:///opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml"))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // create table with column family
//    val admin = new HBaseAdmin(conf)
//    if (!admin.isTableAvailable(tableName)) {
//      print("Creating LBS_3G_DATA Table")
//      val tableDesc = new HTableDescriptor(tableName)
//      tableDesc.addFamily(new HColumnDescriptor("cf1".getBytes))
//      admin.createTable(tableDesc)
//    } else {
//      print("Table already exists!!")
//      val columnDesc = new HColumnDescriptor("cf1")
//      admin.disableTable(Bytes.toBytes(tableName))
//      admin.addColumn(tableName, columnDesc)
//      admin.enableTable(Bytes.toBytes(tableName))
//    }

    //put data into table
    val myTable = new HTable(conf, tableName)
//    for (i <- 0 to 5) {
//      var p = new Put(new String("row" + i).getBytes)
//      p.add("cf1".getBytes, "column-1".getBytes, new String(
//        "value " + i).getBytes)
//      myTable.put(p)
//    }
    var p = new Put(new String(rowkey).getBytes)
    p.add(cf.getBytes, column.getBytes, new String(
      value).getBytes)
    myTable.put(p)
    myTable.flushCommits()
  }
}
