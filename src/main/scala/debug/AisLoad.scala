package debug

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, round, to_timestamp, udf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

import scala.math._
/*
--conf "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64"

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/ && /home/vadim/MyExp/spark-3.2.2-bin-hadoop3.2-scala2.13/bin/spark-submit --class batch.AisLoad   --master spark://vadim-comp:7077 --executor-memory 2G --total-executor-cores 10 /home/vadim/MyExp/Diplom/IdeaProjects/dtest/D/target/scala-2.13/D-assembly-0.1.0-SNAPSHOT.jar

 */


object AisLoad {
  private val Distance = 50 //meters
  private val Window_time = 30 // minutes

  val haversine: (Double, Double, Double, Double) => Double = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val R = 6378137
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians
    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }

  val haversineUDF: UserDefinedFunction = udf(haversine)

  def catched_rows(
                    df: DataFrame,
                    meters: Int,
                    window_time:Int,
                    dist_func: UserDefinedFunction = haversineUDF
                  ): DataFrame = {
    //df.repartition(4)
    df.as("df1")
      .join(df.as("df2"),
        dist_func(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")) < meters
          && col("df1.MMSI") =!= col("df2.MMSI")
          && round(functions.abs(to_timestamp(col("df1.BaseDateTime")).cast(LongType) -
          to_timestamp(col("df2.BaseDateTime")).cast(LongType)) / 60) < window_time
        ,
        "inner")
      .withColumn("haversine", haversineUDF(col("df1.LAT"), col("df1.LON"), col("df2.LAT"), col("df2.LON")))
      .select(col("df1.MMSI").as("MMSI1"), col("df2.MMSI").as("MMSI2"), round(col("haversine"), 1).as("haversine"))
  }

  def main(args: Array[String]): Unit = {
    val postgresqlSinkOptionsCatched: Map[String, String] = Map(
      "dbtable" -> "public.catched", // table
      "user" -> "postgres", // Database username
      "password" -> "mysecretpassword", // Password
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5455/postgres"
    )


    val spark = SparkSession
      .builder()
      .appName("spark-test-ais")
      //.config("spark.master", "spark://vadim-comp:7077")
      .config("spark.master", "local[*]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
    //.config("spark.executor.instances", 2)
    //.config("spark.executor.cores", 2)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ais2022_01_01 = spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      //.csv("/home/vadim/MyExp/Diplom/AISDATA/CoastNoaa/AIS_2022_01_01.csv")
      //.csv("/home/vadim/MyExp/Diplom/KAGGLEDATA/archive/t.csv")
      .csv("/home/vadim/MyExp/Diplom/KAGGLEDATA/archive/10000.csv")
    //ais2022_01_01.show()
    //ais2022_01_01.printSchema()
    catched_rows(ais2022_01_01,Distance,Window_time)
      .write
      .format("jdbc")
      .options(postgresqlSinkOptionsCatched)
      .mode(SaveMode.Append)
      .save()




  }
}