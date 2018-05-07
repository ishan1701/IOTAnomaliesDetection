package iotMLlib

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
