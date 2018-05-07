package iotStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.typesafe.config.Config

trait LoadAverageDistanceModels extends Serializable {
  def getAllModelsAverage(fs: FileSystem, path: Path, sc: SparkContext, spark: SparkSession): Map[String, Map[Int, Double]] = {
    var aveMap: Map[String, Map[Int, Double]] = Map()
    var tempMap: Map[Int, Double] = Map()
    val pathObj = fs.listLocatedStatus((path))
    while (pathObj.hasNext()) {
      val modelType = pathObj.next().getPath.toString().split("/")(4)
      val averageDistance = spark.read.parquet(path + "/" + modelType).rdd.map(row => row.mkString(",")).collect()
      for (itr <- averageDistance) {
        val clusterThreshold = ((itr.split(",")(0).toInt -> itr.split(",")(1).toDouble))
        tempMap += (clusterThreshold)
      }
      aveMap += ((modelType, tempMap))
    }
    aveMap
  }

}