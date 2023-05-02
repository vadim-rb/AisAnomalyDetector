package kapp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json, round, to_json, to_timestamp, udf, window}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession, functions}
import utils.AisRecordTime
import utils.Config.{Distance, postgresqlSinkOptionsAis, postgresqlSinkOptionsCatched}

import scala.math.{asin, cos, pow, sin, sqrt}


object Kappy {

  val haversine: (Double, Double, Double, Double) => Double = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val R = 6378137
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians
    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }

  val haversineUDF: UserDefinedFunction = udf(haversine)

  def catched_rows (
                     df: DataFrame,
                     meters: Int,
                     haversineUDF: UserDefinedFunction=haversineUDF
                   ): DataFrame ={
    df.as("df1")
      .join(df.as("df2"),
        haversineUDF(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")) < meters
          && col("df1.MMSI") =!= col("df2.MMSI"),
        "inner")
      .withColumn("haversine", haversineUDF(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")))
      .select(col("df1.MMSI").as("MMSI1"), col("df2.MMSI").as("MMSI2"), round(col("haversine"), 1).as("haversine"))

  }

  def catched_rows_window(
                           batch: DataFrame,
                           meters: Int,
                           dist_func: UserDefinedFunction = haversineUDF
                         ): DataFrame = {
    batch.as("df1")
      .join(batch.as("df2"),
        dist_func(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")) < meters
          && col("df1.MMSI") =!= col("df2.MMSI")
          && to_json(col("df1.window")) === to_json(col("df2.window")),
        "inner")
      .withColumn("haversine", dist_func(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")))
      .select(col("df1.MMSI").as("MMSI1"), col("df2.MMSI").as("MMSI2"), round(col("haversine"), 1).as("haversine"))
  }

  def catched_rows_debug(
                    df: DataFrame,
                    meters: Int,
                    window_time: Int,
                    dist_func: UserDefinedFunction = haversineUDF
                  ): DataFrame = {
    df.as("df1")
      .join(df.as("df2"),
        dist_func(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")) < meters
          && col("df1.MMSI") =!= col("df2.MMSI")
          && round(functions.abs(to_timestamp(col("df1.BaseDateTime")).cast(LongType) -
          to_timestamp(col("df2.BaseDateTime")).cast(LongType)) / 60) < window_time
        ,
        "inner")
      .withColumn("haversine", dist_func(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")))
      .select(
        col("df1.MMSI").as("MMSI1"),
        col("df1.LAT").as("LAT1"),
        col("df1.LON").as("LON1"),
        col("df1.BaseDateTime").as("BaseDateTime1"),
        col("df2.MMSI").as("MMSI2"),
        col("df2.LAT").as("LAT2"),
        col("df2.LON").as("LON2"),
        col("df2.BaseDateTime").as("BaseDateTime2"),
        round(col("haversine"), 1).as("haversine"))
  }

  def multiple_insert(batch: DataFrame, batchId: Long): Unit ={

    val crw = catched_rows_window(batch, Distance)

    crw.persist()

    crw
      .write
      .format("jdbc")
      .options(postgresqlSinkOptionsCatched)
      .mode(SaveMode.Append)
      .save()

    crw
      .write
      .format("console")
      .save()

    crw.unpersist()
  }

  def main(args: Array[String]): Unit = {
    //case class convert to schema
    val encoderSchema = Encoders.product[AisRecordTime].schema
    //encoderSchema.printTreeString()

    val spark = SparkSession
      .builder()
      .appName("spark-kapp")
      .config("spark.master", "local[*]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "mytopic")
      .option("includeTimestamp", value = true)
      .load()

    val ais_records = df.selectExpr("CAST(value AS STRING)")
    val ais_data = ais_records.select(from_json(col("value"), encoderSchema).as("data"))
      .select("data.*")

    val query_ais_data = ais_data.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("jdbc")
          .options(postgresqlSinkOptionsAis)
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    val windowedAis = ais_data
      .withColumn("BaseDateTime",to_timestamp(col("BaseDateTime")))
      .withWatermark("BaseDateTime", "10 minutes")
      .groupBy(
      window(col("BaseDateTime"),"60 seconds","30 seconds")
      ,col("MMSI")
      ,col ("LAT")
      ,col("LON")
      ,col("BaseDateTime")
    ).count()

    val query_window = windowedAis
      .writeStream
      .outputMode("complete")
      .foreachBatch(multiple_insert _)
      .start()

    query_ais_data.awaitTermination()
    query_window.awaitTermination()

  }
}

/*
DEBUG
+---------+-------------------+-------------------+-------------------+---------+-------------------+-------------------+-------------------+---------+
|    MMSI1|               LAT1|               LON1|      BaseDateTime1|    MMSI2|               LAT2|               LON2|      BaseDateTime2|haversine|
+---------+-------------------+-------------------+-------------------+---------+-------------------+-------------------+-------------------+---------+
|171443472|      13.1806077693|       -59.21524401|2023-04-25T16:59:29|146061087| 13.180601567592099| -59.21543996709386|2023-04-25T16:59:29|     21.3|
|171443472|        13.16714247|       -59.21377537|2023-04-25T16:59:29|146061087| 13.167194695407778| -59.21390596300906|2023-04-25T16:59:29|     15.3|
|171443472|        13.15377606|       -59.21241869|2023-04-25T16:59:29|146061087| 13.153787823223457|-59.212371958924265|2023-04-25T16:59:29|      5.2|
|169066065|       -22.82689101|        14.05470874|2023-04-25T16:59:29|130784060|-22.826820400544044| 14.054946899414064|2023-04-25T16:59:30|     25.7|
|146061087| 13.180601567592099| -59.21543996709386|2023-04-25T16:59:29|171443472|      13.1806077693|       -59.21524401|2023-04-25T16:59:29|     21.3|
|146061087| 13.167194695407778| -59.21390596300906|2023-04-25T16:59:29|171443472|        13.16714247|       -59.21377537|2023-04-25T16:59:29|     15.3|
|146061087| 13.153787823223457|-59.212371958924265|2023-04-25T16:59:29|171443472|        13.15377606|       -59.21241869|2023-04-25T16:59:29|      5.2|
|130784060|-22.826820400544044| 14.054946899414064|2023-04-25T16:59:30|169066065|       -22.82689101|        14.05470874|2023-04-25T16:59:29|     25.7|
+---------+-------------------+-------------------+-------------------+---------+-------------------+-------------------+-------------------+---------+

 */
