package com.xxx.common.utils

import org.apache.hadoop.hbase.client.{Put, HBaseAdmin}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Writable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

object HbaseCommonUtils {
   /**
   * loadProperties
   * 
   */
  private val prop = loadProperties("hbase/config.properties")
  private val QUORUM = prop.getProperty("hbase.zookeeper.quorum",null)
  private val PORT = prop.getProperty("hbase.zookeeper.property.clientPort",null)
  private val MASTER = prop.getProperty("hbase.master",null)

  val conf = {
    val conf = HBaseConfiguration.create()
    if(QUORUM != null)
      conf.set("hbase.zookeeper.property.clientPort", PORT)
    if(PORT != null)
      conf.set("hbase.zookeeper.quorum", QUORUM)
    if(MASTER != null)
      conf.set("hbase.master", MASTER)
    conf
  }

  /**
   * @param table-Name
   * @param column Family 
   */
  def createTable(tableName:String,columnFamilys:String*){
    val admin = new HBaseAdmin(conf)
    if(admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for(family<-columnFamilys) tableDescriptor.addFamily(new HColumnDescriptor(family.getBytes).setCompressionType(Compression.Algorithm.GZ))
    admin.createTable(tableDescriptor)
  }

  def transHbaseTable2RDD(sc:SparkContext, tableName: String) ={
    val hbaseConf = HbaseCommonUtils.conf
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,tableName)
    sc.newAPIHadoopRDD(hbaseConf,classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result])
  }

  def transHbaseRDD2DF(hc:HiveContext,keyName:String,qualifiers:Array[(String,String)],
                         hbaseRDD:RDD[(ImmutableBytesWritable,org.apache.hadoop.hbase.client.Result)]) = {
    val schema = getSchemaFromQualifiers(keyName,qualifiers)
    val rowRDD = hbaseRDD.map(x=>hbaseResult2DFRow(x._2,qualifiers))
    hc.createDataFrame(rowRDD,schema)
  }

  /**
    * hbase table to DF row
    * @param hc,keyName ,qualifiers,tableName
    */
  def transHbaseTable2DF(hc:HiveContext,keyName:String,qualifiers:Array[(String,String)], tableName: String) = {
    val schema = getSchemaFromQualifiers(keyName,qualifiers)
    val rowRDD = transHbaseTable2RDD(hc.sparkContext,tableName).map(x=>hbaseResult2DFRow(x._2,qualifiers))
    hc.createDataFrame(rowRDD,schema)
  }

  /**
    * hbase to DF row
    * @param result ,qualifiers
    */
  def hbaseResult2DFRow(result:org.apache.hadoop.hbase.client.Result,qualifiers:Array[(String,String)]) ={
    val value = Array(Bytes.toString(result.getRow)) ++
      qualifiers.map{
        case (column,dataType) =>
          val Array(family,col) = column.split(":")
          val isContainsColumn = result.containsColumn(Bytes.toBytes(family),Bytes.toBytes(col))
          dataType match {
            case "String" =>
              if(isContainsColumn)
                Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                ""
            case "Double" =>
              if(isContainsColumn)
                Bytes.toDouble(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                0.0
            case "Int" =>
              if(isContainsColumn)
                Bytes.toInt(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                0
            case "Long" =>
              if(isContainsColumn)
                Bytes.toLong(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                0l
            case "Boolean" =>
              if(isContainsColumn)
                Bytes.toBoolean(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                false
            case _ =>
              if(isContainsColumn)
                Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
              else
                ""
          }
      }
    Row(value:_*)
  }

  /**
    * Schema from qualifiers
    * @param keyName ,qualifiers
    */
  def getSchemaFromQualifiers(keyName:String,qualifiers:Array[(String,String)])= {
    StructType(Array(StructField(keyName,StringType)) ++ qualifiers.map{
      case (column,dataType) =>
        dataType match {
          case "String" => StructField(column.replace(":","_"),StringType)
          case "Double" => StructField(column.replace(":","_"),DoubleType)
          case "Int" => StructField(column.replace(":","_"),IntegerType)
          case "Long" => StructField(column.replace(":","_"),LongType)
          case "Boolean" => StructField(column.replace(":","_"),BooleanType)
          case _ => StructField(column.replace(":","_"),StringType)
        }
    })
  }

  /**
    * FROM DataFrame WRITE INTO Hbase,DataFrame NEED A keyName,AS Hbase'S rowKey
    * @param df
    */
  def writeHbaseFromDF(df:DataFrame,tableName: String,keyName:String,qualifiers:Array[(String,String)]) ={
    val hbaseConf = HbaseCommonUtils.conf
    val job = org.apache.hadoop.mapreduce.Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Writable])
    job.getConfiguration.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)

    val rdd = df.rdd.map{
      row =>
        val p = new Put(Bytes.toBytes(row.getAs[String](keyName)))
        qualifiers.foreach{
          case (column,dataType) =>
            val Array(family,col) = column.split(":")
            dataType match {
              case "String" =>
                val value = row.getAs[String](family + "_" + col)
                if(value != "")
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              case "Double" =>
                val value = row.getAs[Double](family + "_" + col)
                if(value != 0.0)
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              case "Int" =>
                val value = row.getAs[Int](family + "_" + col)
                if(value != 0)
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              case "Long" =>
                val value = row.getAs[Long](family + "_" + col)
                if(value != 0l)
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              case "Boolean" =>
                val value = row.getAs[Boolean](family + "_" + col)
                if(value)
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
              case _ =>
                val value = row.getAs[String](family + "_" + col)
                if(value != "")
                  p.add(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(value))
            }
        }
        (new ImmutableBytesWritable, p)
    }.filter(!_._2.isEmpty)
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
