package utils

import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv._
import java.io.File
import kantan.csv.ops._
import kantan.csv.generic._
import io.scalaland.chimney.dsl._
import scala.io.Codec.fallbackSystemCodec
import Config.{path_to_csv, routes_csv}

object CreateCsv {

  def main(args: Array[String]): Unit = {
    val rand = scala.util.Random
    implicit val AisEncoder: RowEncoder[AisRecord] = RowEncoder.encoder(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)((p: AisRecord) => (
      p.MMSI,
      p.BaseDateTime,
      p.LAT,
      p.LON,
      p.SOG,
      p.COG,
      p.Heading,
      p.VesselName,
      p.IMO,
      p.CallSign,
      p.VesselType,
      p.Status,
      p.Length,
      p.Width,
      p.Draft,
      p.Cargo,
      p.TransceiverClass
    ))
    routes_csv foreach((filename:String) => {
      println(filename)
      val rand_mmsi = rand.between(111111111,222222222).toInt
      val rawData: java.net.URL = getClass.getResource(s"/$filename")
      val reader = rawData.asCsvReader[Gps](rfc.withoutHeader)
      var wr:List[AisRecord] = List()
      reader.foreach{
        case Left(s) => println(s"Error: $s")
        case Right(i) =>
          val mod = i.into[AisRecord]
            .withFieldComputed(_.MMSI, _ => rand_mmsi)
            .withFieldConst(_.BaseDateTime, None)
            .withFieldConst(_.SOG, None)
            .withFieldConst(_.COG, None)
            .withFieldConst(_.Heading, None)
            .withFieldConst(_.VesselName, None)
            .withFieldConst(_.IMO, None)
            .withFieldConst(_.CallSign, None)
            .withFieldConst(_.VesselType, None)
            .withFieldConst(_.Status, None)
            .withFieldConst(_.Length, None)
            .withFieldConst(_.Width, None)
            .withFieldConst(_.Draft, None)
            .withFieldConst(_.Cargo, None)
            .withFieldConst(_.TransceiverClass, None)
            .transform
          wr = wr :+ mod
      }
      new File(s"$path_to_csv$filename").writeCsv[AisRecord](wr,rfc)
    })
  }

}
