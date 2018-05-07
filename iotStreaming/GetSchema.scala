package iotStreaming

import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{ DoubleType, LongType }

trait GetSchema {
  def getPLCSchema(): StructType = {
    val schema = StructType(Array(
      StructField("Machine Id", LongType, true),
      StructField("value", StructType(Array(
        StructField("humidity", DoubleType, true),
        StructField("speed", DoubleType, true),
        StructField("temperature", DoubleType, true),
        StructField("vibration", DoubleType, true),
        StructField("viscosity", DoubleType, true),
        StructField("weight", DoubleType, true))), true)))
    schema
  }

  def getraspberrySchema(): StructType = {
    val schema = StructType(Array(
      StructField("vibration-detected", DoubleType, true),
      StructField("temperature", DoubleType, true),
      StructField("Carbon_monoxide-detected", DoubleType, true),
      StructField("humidity", DoubleType, true)))
    schema
  }
}

