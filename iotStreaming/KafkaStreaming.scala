package iotStreaming

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.{ Logger, Level }
import com.typesafe.config._
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object KafkaStreaming extends GetSchema with LoadAverageDistanceModels{
    def main(args: Array[String]): Unit = {

    //Setup Spark-Context and Streaming Context
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("IoT Injestion")
      .set("spark.cassandra.connection.host", "192.168.0.8")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val config = ConfigFactory.load("resources/application.properties")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    val logger = Logger.getLogger(KafkaStreaming.getClass)
    logger.info("supressing akka logs. Please enable during debug")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    logger.info("Conf object has been set with  Cassandra")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    logger.info("Loading hadoop confuguration")
    val sc = spark.sparkContext
    
    val modelMapBC = sc.broadcast(Utils.loadUnsupervisedModels(fs, new Path(config.getString("modelPath")), sc))
    logger.info("loading the models as broadcast variable")
    val modelsAverageDistanceBC = sc.broadcast(getAllModelsAverage(fs, new Path(config.getString("modelThresholdPath")), sc, spark))
    logger.info("loading model's average distance as a broadcast variable")
    val raspberryTopic = config.getConfig("Raspberry").getString("topics")
    

    // Fetching Pi Schema and Data
    val zkQuorum = config.getConfig("IOT").getString("zkQuorum")
    val piTopics = Map(config.getConfig("Raspberry").getString("topics") -> 1)
    val piData = KafkaUtils.createStream(ssc, zkQuorum, "consumer1", piTopics).map(tuple => tuple._2)
    val piSchema = getraspberrySchema()
    Utils.processRaspberry(piData, piSchema, spark)

    // Fetching PLC Schema and Data
    val PLCTopics = Map(config.getConfig("PLC").getString("topics") -> 1)
    val PLCData = KafkaUtils.createStream(ssc, zkQuorum, "consumer2", PLCTopics).map(tuple => tuple._2)
    val PLCSchema = getPLCSchema()
    Utils.processPLC(PLCData, PLCSchema, spark, modelMapBC, modelsAverageDistanceBC)

    ssc.start()
    ssc.awaitTermination()
  }

}