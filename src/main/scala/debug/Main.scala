package debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, udf}

import scala.math._
/*
https://en.wikipedia.org/wiki/Automatic_identification_system

https://www.svb24.com/en/guide/everything-there-is-to-know-about-ais.html

https://www.marinetraffic.com/blog/information-transmitted-via-ais-signal/

https://www.itu.int/dms_pubrec/itu-r/rec/m/R-REC-M.1371-4-201004-S!!PDF-R.pdf

https://ru.wikipedia.org/wiki/%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F_%D0%B8%D0%B4%D0%B5%D0%BD%D1%82%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%86%D0%B8%D0%BE%D0%BD%D0%BD%D0%B0%D1%8F_%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0#cite_note-3

Номер MMSI ( M aritime Mobile Service Identity ) — это уникальный девятизначный номер для идентификации судна.
Он запрограммирован во всех системах АИС и электронике УКВ на борту судна и предоставляет номер,
соответствующий международным стандартам, для связи с судном. Этот номер присваивается судну,
а не физическому лицу, соответствующими органами в стране регистрации судна.
Все номера MMSI судов имеют формат MIDXXXXXX.
Первые три цифры называются MID (цифры морского опознавания) и обозначают национальность
(см. Таблицу морских опознавательных знаков МСЭ: https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid. aspx).
Последние шесть цифр используются для уникальной идентификации судна.

Обратите внимание: система AIS использует MMSI, а не номер IMO,
для идентификации судна. Номер ИМО является частью статической информации АИС,
предоставляемой экипажем судна, и может передаваться дополнительно.
Таким образом, если вы ищете судно в базе данных судов FleetMon по его номеру IMO, вы можете найти более одной записи,
потому что в прошлом судно имело разные номера MMSI. В некоторых случаях возможно изменение количества судов в MMSI,
например, судно продано или зафрахтовано на длительный срок, и меняется флаг. Затем изменится и MMSI,
потому что судну нужен другой MID. Вы найдете одно и то же судно с разными номерами MMSI в базе данных судов.

https://help.fleetmon.com/en/articles/6589334-difference-between-heading-hdg-and-course-over-ground-cog
 */


object Main {
  /*
  MMSI - Maritime Mobile Service Identity (MMSI) или опознаватель морской подвижной службы — это индивидуальный девятизначный номер судна или морской службы.
  MMSI - Maritime Mobile Service Identity. Это 9-ти значный индивидуальный номер, закрепленный за судном или морской службой. Первые три цифры номера обозначают страну регистрации. Для России это 273.

  IMO Номер ИМО — уникальный идентификатор судна.
  Номер ИМО состоит из трёхбуквенной латинской аббревиатуры «IMO», за которой следует число из семи цифр. Первые шесть из них являются уникальным порядковым номером судна, а седьмая цифра — контрольная.
  номер ИМО должен быть постоянно обозначен на видном месте на корпусе судна или надстройке.
  Номер ИМО присваивается морским торговым судам валовой вместимостью от 100 тонн

  BaseDateTime время передачи данных Position Timestamp in UTC

  LAT широта

  LON долгота

  SOG Speed over Ground  Скорость судна относительно грунта
  скорость судна за один час относительно земли или любого другого неподвижного объекта
  Измеряется в узлах
  Узел — Knot — единица измерения скорости судна. Морская миля в час.
  международная морская миля (International Nautical Mile) равна ровно 1852 метрам.
  Распространённость узла как единицы измерения связана со значительным удобством его применения в навигационных расчётах,
  судно, идущее на скорости в 1 узел вдоль меридиана, за 1 час проходит 1 угловую минуту географической широты.

  COG Course Over Ground Курс относительно земли
  угол между путём судна и меридианом (истинным севером)

  Heading угол между плоскостью меридиана и диаметральной плоскостью судна
  По сути показывает направление носа  относительно севера.

  VesselName Имя Судна

  CallSign Позывной
  уникальный позывной судна назначается национальными властями и позволяет вам идентифицировать судно или плавсредство и при необходимости вызывать их.
  радиопозывной
  В приниципе может быть необязательным

  VesselType Тип корабля
  https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
  https://api.vtexplorer.com/docs/ref-aistypes.html
  https://api.vtexplorer.com/docs/ref-aistypes.html

  Status
  AIS Navigational Status https://datalastic.com/blog/ais-navigational-status/
  https://api.vtexplorer.com/docs/ref-navstat.html
  https://help.marinetraffic.com/hc/en-us/articles/203990998-What-is-the-significance-of-the-AIS-Navigational-Status-Values-

  Length Длина

  Width Ширина

  Draft Осадка глубина погружения судна в воду. Расстояние самой углубленной точки подводной части судна от поверхности воды и есть осадка
  Введена в начале рейса с использованием максимальной осадки для рейса и изменена по мере необходимости (например, в результате дебалластировки перед заходом в порт)
  meters

  Cargo
  Tug буксировать
  Tow Буксировка - это что-то тянуть веревкой (или чем-то подобным).

  TransceiverClass Трансиверы класса A и B
  Международные морские суда валовой вместимостью более 300 и некоторые пассажирские суда подпадают под действие Конвенции СОЛАС. Эти корабли должны быть оборудованы трансиверами класса А.
  Приемопередатчики AIS класса A имеют мощность передачи до 12,5 Вт, что больше, чем у оборудования класса B, используемого на прогулочных судах. Таким образом, приемопередатчики класса А могут отправлять и получать данные на большие расстояния.

  ля спортивных лодок, используемых только для отдыха или рыбалки, приемопередатчики AIS не требуются.
  Однако, поскольку приемопередатчик AIS на борту имеет много преимуществ, доступны так называемые приемопередатчики AIS класса B.
  Приемопередатчики AIS класса B обычно дешевле, чем приемопередатчики класса A, поскольку предъявляются менее строгие эксплуатационные требования. Приемопередатчики класса B уменьшены для включения наиболее важных данных. Часто передаются только номер MMSI судна, текущая позиция, курс и размер. Это делает приемопередатчики класса B очень простыми в эксплуатации.
   */

  val haversine = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    val R = 6378137
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians
    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark-test-ais")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ais2022_01_01 = spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      //.csv("/home/vadim/MyExp/Diplom/AISDATA/CoastNoaa/AIS_2022_01_01.csv")
      .csv("/home/vadim/MyExp/Diplom/KAGGLEDATA/archive/t.csv")

    val haversineUDF = udf(haversine)
    //ais2022_01_01.show()
    val df = ais2022_01_01

    val selfDF = df.as("df1")
      .join(df.as("df2"),
        haversineUDF(col("df1.LAT"),col("df1.LON"),col("df2.LAT"),col("df2.LON"))<100
          && col("df1.MMSI") =!= col("df2.MMSI")
        ,
        "inner")
      .withColumn("haversine",haversineUDF(col("df1.LAT"),col("df1.LON"),col("df2.LAT"),col("df2.LON")))
      .select(col("df1.MMSI").as("MMSI1"),col("df2.MMSI").as("MMSI2"),round(col("haversine"),1).as("haversine"))
    selfDF.show()

    /*
    val df = ais2022_01_01
      .select(col("LAT"),col("LON"),col("VesselName"))
      .where(col("MMSI") === 368084090)
    df.show()

    df
      .coalesce(1)
      .write
      .option("header",value = true)
      .csv("/home/vadim/MyExp/Diplom/AISDATA/CoastNoaa/368084090")

     */
  }
}