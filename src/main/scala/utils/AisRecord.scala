package utils

case class AisRecord(
                      MMSI:Int,
                      BaseDateTime:Option[String],
                      LAT:Double,
                      LON:Double,
                      SOG:Option[Double],
                      COG:Option[Double],
                      Heading:Option[Double],
                      VesselName:Option[String],
                      IMO:Option[String],
                      CallSign:Option[String],
                      VesselType:Option[Int],
                      Status:Option[Int],
                      Length:Option[Double],
                      Width:Option[Double],
                      Draft:Option[Double],
                      Cargo:Option[Int],
                      TransceiverClass:Option[String]
                    )
