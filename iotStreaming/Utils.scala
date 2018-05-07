package iotStreaming

import org.apache.spark.rdd
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.sql.cassandra
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel

object Utils extends GetSchema {
  var nuMCL = (-1)
  def findMinDistance(dataVector: org.apache.spark.mllib.linalg.Vector, clusterCenter: Array[Vector]): (Int, Double) = {
    var distMap: scala.collection.immutable.Map[Int, Double] = Map()
    for (itr <- clusterCenter) {
      distMap = distMap + findDistance(dataVector, itr)
    }
    nuMCL = (-1)
    distMap.minBy(x => x._2)
  }
  def findDistance(arr1: org.apache.spark.mllib.linalg.Vector, arr2: org.apache.spark.mllib.linalg.Vector): (Int, Double) = {
    nuMCL = nuMCL + 1
    val interArr = arr1.toArray.zip(arr2.toArray)
    (nuMCL, math.sqrt(interArr.map(arr => ((arr._1 - arr._2) * (arr._1 - arr._2))).reduce((acc, value) => acc + value)))
  }
  def storeRaspberryCassandra(selectedRaspberryData: org.apache.spark.sql.DataFrame) = {
    selectedRaspberryData.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "IoT", "table" -> "raspberrypitable", "cluster" -> "IOT Cluster"))
      .mode(org.apache.spark.sql.SaveMode.Append).save()
  }
  def storePLCCassandra(selectedPLCData: org.apache.spark.sql.DataFrame) = {
    selectedPLCData.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "IoT", "table" -> "plctable", "cluster" -> "IOT Cluster"))
      .mode(org.apache.spark.sql.SaveMode.Append).save()
  }
  def loadUnsupervisedModels(fs: FileSystem, path: Path, sc: SparkContext): (Map[String, KMeansModel]) = {
    var modelMaps: Map[String, KMeansModel] = Map()
    val pathObj = fs.listLocatedStatus(path)
    while (pathObj.hasNext()) {
      val path = pathObj.next().getPath.toString()
      val modelName = path.split("/")(4)
      val model = org.apache.spark.mllib.clustering.KMeansModel.load(sc, path)
      modelMaps += ((modelName, model))
    }
    modelMaps
  }
  def parsePLCDATA(rawData: org.apache.spark.sql.DataFrame): org.apache.spark.rdd.RDD[PLCDataModel] = {

    val plcRdd = rawData.select(
      "value.humidity",
      "value.speed",
      "value.temperature", "value.vibration", "value.viscosity", "value.weight")
      .rdd.map(row => row.mkString(","))
      .map(record => record.split(","))
      .map(record => PLCDataModel(record(0).toDouble, record(1).toDouble, record(2).toDouble, record(3).toDouble, record(4).toDouble, record(5).toDouble))
    plcRdd
  }
  def processPLC(
    PLCData:                 org.apache.spark.streaming.dstream.DStream[String],
    PLCSchema:               org.apache.spark.sql.types.StructType,
    spark:                   org.apache.spark.sql.SparkSession,
    modelMapBC:              org.apache.spark.broadcast.Broadcast[Map[String, KMeansModel]],
    modelsAverageDistanceBC: org.apache.spark.broadcast.Broadcast[Map[String, Map[Int, Double]]]) = {

    PLCData.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        import spark.implicits._
        val df = spark.read.schema(PLCSchema).json(rdd)
        df.createOrReplaceTempView("t2")
        val selectedPLCData = spark.sql(""" select `Machine Id` ,value.* from t2 """)
        storeRaspberryCassandra(selectedPLCData)
        val rawData = spark.read.schema(getPLCSchema).json(rdd)
        val parsedData = parsePLCDATA(rawData).map(row => Vectors.dense(row.humidity, row.speed, row.temperature, row.vibration, row.viscosity, row.weigh))
        val anomaliesData = parsedData.map(data => {
          val prediction = modelMapBC.value("PLCAnomaliesDetectionModel").predict(data)
          if (Utils.findMinDistance(data, modelMapBC.value("PLCAnomaliesDetectionModel").clusterCenters)._2 > 3 * (modelsAverageDistanceBC.value("PLCAnomaliesDetectionModel").getOrElse(0, 0.0)))
            PLCAnomalies(data, prediction, true)
          else
            PLCAnomalies(data, prediction, false)

        }).toDF()

        //anomaliesData.show()
        anomaliesData.filter("anomaliesDetected").show()

      }
    })
  }

  def processRaspberry(
    RaspberryData:   org.apache.spark.streaming.dstream.DStream[String],
    RaspberrySchema: org.apache.spark.sql.types.StructType, spark: org.apache.spark.sql.SparkSession) = {

    RaspberryData.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val df = spark.read.schema(RaspberrySchema).json(rdd)
        df.createOrReplaceTempView("t1")
        val selectedRaspberryData = spark.sql(""" select `vibration-detected` vibration_detected,
											temperature temperature,
											`Carbon_monoxide-detected` carbon_monoxide_detected,
											humidity humidity from t1 """)
        storeRaspberryCassandra(selectedRaspberryData)
      }
    })
  }

}