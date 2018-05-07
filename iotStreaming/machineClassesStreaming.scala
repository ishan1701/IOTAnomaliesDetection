package iotStreaming

import org.apache.spark.mllib.linalg.{ Vector, Vectors }

case class PLCDataModel(humidity: Double,
  speed: Double,
  temperature: Double,
  vibration: Double,
  viscosity: Double,
  weigh: Double)

case class raspberryPiModel(humidity: Double,
  temperature: Double,
  carbonMonoxide: Double,
  vibration: Double)

case class PLCAnomalies(data: org.apache.spark.mllib.linalg.Vector,
  prediction: Int,
  anomaliesDetected: Boolean)
