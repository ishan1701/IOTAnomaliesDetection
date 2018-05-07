package iotMLlib

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.typesafe.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.FileSystem

case class AverageDistance(cluster: Int, threshold: Double)
object AnomaliesDetctionModelBuild {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Anomalies detection Model Building")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val config = ConfigFactory.load("resources/application.properties")
    val logger = Logger.getLogger(AnomaliesDetctionModelBuild.getClass)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    Utils.deleteModelPath(fs, config)
    logger.info("supressing akka logs")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val modelData = spark.read.json(spark.sparkContext.wholeTextFiles(args(0)).values)
    val parsedData = Utils.parsePLCDATA(modelData).map(data => Vectors.dense(data.humidity, data.speed, data.temperature, data.vibration, data.viscosity, data.weigh)).cache()
    val numClusters = config.getConfig("PLC").getString("numOfClusters").toInt
    val numIteration = config.getConfig("PLC").getString("numOfIteration").toInt
    val plcModel = KMeans.train(parsedData, numClusters, numIteration)
    logger.info("saving KMeans model")
    Utils.saveModel(plcModel, spark.sparkContext, config)

    val pointsDistance = parsedData.map(Vector => (Vector, Utils.findMinDistance(Vector, plcModel.clusterCenters)))
    val clusterDistanceTuple = pointsDistance.map { case (a, (cluster, distance)) => (cluster, distance) }
    val averageDistance = clusterDistanceTuple.aggregateByKey((0.0, 0.0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (it1, it2) => ((it1._1 + it2._1), it1._2 + it2._2))
      .map(aggregateDistanceTuple => (aggregateDistanceTuple._1, aggregateDistanceTuple._2._1 / aggregateDistanceTuple._2._2))
    val clustersAverageDF = averageDistance.map { case (cluster, distance) => AverageDistance(cluster, distance) }.toDF().coalesce(1)
    clustersAverageDF.write.mode(SaveMode.Overwrite).save(config.getConfig("PLC").getString("modelThresholdPath"))

  }

}