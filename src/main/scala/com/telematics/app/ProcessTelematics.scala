package com.telematics.app

import java.io.{BufferedInputStream, FileInputStream}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.{FileSystems, Files}
import java.util.zip.GZIPInputStream
import scala.io.Source
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import scala.collection.JavaConverters._

case class TelematicsData(
     data: Array[Data],
     trip_id: String
   )

case class Data(
     distance: Double,
     sub_id: String,
     vehicle: String,
     link_id: String,
     time: String,
     lat: Double,
     lng: Double,
     speed: Double
   )

class ProcessTelematics(spark: SparkSession) extends java.io.Serializable {

  def getMilliSec(value: Seq[String]) : Long  = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
    val datetimeStart = formatter.parseDateTime(value.head.split("\\+").head)
    val datetimeEnd = formatter.parseDateTime(value.last.split("\\+").head)
    ((datetimeEnd.getMillis-datetimeStart.getMillis))

  }

  def distinctValue(value: Seq[String]) : String = {
    value.head
  }
  val getMilliSecUDF = udf[Long,Seq[String]](getMilliSec)
  val distinctValueUDF = udf[String,Seq[String]](distinctValue)


  def getGzipData(file: String): List[String] = {
    val in   = new GZIPInputStream(new FileInputStream(file))
    val data = Source.fromInputStream(in).getLines().mkString.split("\n")
      .toList
      .map { line =>
      line.replaceFirst("[}]$",s""",\"trip_id\":\"${file.split("/").last.replace("-processed.json.gz","")}\"}""")
    }
    data
  }

  def getListOfFiles(dataPath:String): Seq[String] ={
    val file = FileSystems.getDefault.getPath(dataPath)
    Files.walk(file).iterator().asScala.map(x => x.toString).filter(_.contains("processed")).toSeq
  }

  def getSampleDataset(dataPath:String): DataFrame ={
    import spark.implicits._

    val listOfFiles: Seq[String] = getListOfFiles(dataPath)
    val listOfFileDataset: RDD[String] = spark.sparkContext.parallelize(listOfFiles).flatMap(file => getGzipData(file)).distinct()
    val data = spark.read.json(listOfFileDataset)
      .sample(true, 0.01)
      .as[TelematicsData]
    val sampleData = data.select(explode($"data").as("tmp"), $"trip_id")
      .select($"tmp.*", $"trip_id")
    sampleData

  }

  def getTotalTimeAndTotalDist(dataPath:String, outPath:String):Unit={
    val distanceAndSecondsPerSubid = getSampleDataset(dataPath).orderBy("time")
      .groupBy("sub_id")
      .agg(sum("distance") as "distance", getMilliSecUDF(collect_list("time")) as "seconds")
      .drop("sub_id")
    distanceAndSecondsPerSubid.agg(sum("distance")/1000 as "total_distance_in_km",sum("seconds")/1000 as "total_seconds")
      .write.mode("append").json(s"${outPath}_2_${scala.util.Random.nextInt(10000)}")
  }

  def getDistanceTimeSpeedPerTrip(dataPath:String, outPath:String):Unit= {
    val distanceAndHourPerTrip = getSampleDataset(dataPath).orderBy("time")
      .groupBy("trip_id")
      .agg(sum("distance")/1000 as "distance_in_km", getMilliSecUDF(collect_list("time"))/1000/60/60 as "hours")
    val distanceHourSpeedPerTrip = distanceAndHourPerTrip.withColumn("avg_speed_in_kph", distanceAndHourPerTrip("distance_in_km")/distanceAndHourPerTrip("hours"))
    distanceHourSpeedPerTrip.repartition(1).write.mode("append").json(s"${outPath}_3a_${scala.util.Random.nextInt(10000)}")
  }

  def getDistanceTimeSpeedPerDriver(dataPath:String, outPath:String):Unit= {
    val distanceAndHourPerDriver = getSampleDataset(dataPath).orderBy("time")
      .groupBy("trip_id")
      .agg(
        distinctValueUDF(collect_list("sub_id")) as "sub_id",
        sum("distance")/1000 as "distance_in_km",
        getMilliSecUDF(collect_list("time"))/1000/60/60 as "hours",
        avg("speed") as "avg_speed")
    val distanceHourSpeedPerDriver = distanceAndHourPerDriver
      .withColumn("avg_speed_in_kph", distanceAndHourPerDriver("distance_in_km")/distanceAndHourPerDriver("hours"))
      .groupBy("sub_id")
      .agg(sum("distance_in_km") as "distance_in_km",sum("hours") as "hours", avg("avg_speed_in_kph") as "avg_speed_in_kph")
    distanceHourSpeedPerDriver.repartition(1).write.mode("append").json(s"${outPath}_3b_${scala.util.Random.nextInt(10000)}")
  }
  def getMaxSpeedPerDriver(dataPath:String, outPath:String):Unit= {
    val distanceHourSpeedPerTrip = getSampleDataset(dataPath).orderBy("time")
      .groupBy("sub_id")
      .agg(max("speed") as "max_speed")
    distanceHourSpeedPerTrip.repartition(1).write.mode("append").json(s"${outPath}_4_${scala.util.Random.nextInt(10000)}")
  }
}

object ProcessTelematics {

  val customClassesToRegisterWithKryo: Array[Class[_]] = Array.empty

  def main(args: Array[String]): Unit ={
    println("HelloWorld")
    val process = new ProcessTelematics(createSpark(args(0), args(1)))
    process.getSampleDataset(args(2)).write.mode("append").json(s"${args(3)}_1_${scala.util.Random.nextInt(10000)}")
    process.getTotalTimeAndTotalDist(args(2), args(3))
    process.getDistanceTimeSpeedPerTrip(args(2), args(3))
    process.getDistanceTimeSpeedPerDriver(args(2), args(3))
    process.getMaxSpeedPerDriver(args(2), args(3))
  }
  def createSpark(master: String, mode : String): SparkSession  = {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName("telematicsMetrics")
      .set("spark.submit.deployMode", mode)
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

}
