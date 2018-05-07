package iotMLlib

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.hadoop.fs.FileSystem
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path

object Utils extends Serializable {
  var nuMCL = (-1)
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
  def saveModel(model: org.apache.spark.mllib.clustering.KMeansModel, sc: org.apache.spark.SparkContext, config: com.typesafe.config.Config) = {
    model.save(sc, config.getConfig("PLC").getString("modelPath"))
  }

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

  def deleteModelPath(fs: FileSystem, config: Config) = {

    if (fs.exists(new Path(config.getConfig("PLC").getString("modelPath")))) {
      fs.delete(new Path(config.getConfig("PLC").getString("modelPath")))
    }

  }

}