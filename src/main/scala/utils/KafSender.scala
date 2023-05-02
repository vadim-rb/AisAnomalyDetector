package utils

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import io.scalaland.chimney.dsl.TransformerOps
import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv._

import java.io.File
import kantan.csv.ops._
import kantan.csv.generic._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import utils.Config.{bootstrap_servers, linesMap, maintopic, path_to_csv, routes_csv, sending_time}

import java.io.File
import java.util.Properties
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import io.scalaland.chimney.dsl._
import org.apache.commons.math3.util.FastMath.round

object KafSender {

  def main(args: Array[String]): Unit = {

    implicit val codecOut: JsonValueCodec[AisRecordTime] = JsonCodecMaker.make
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap_servers)
    val rand = scala.util.Random
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")


    val threads = routes_csv
      .map(fn => {
        new Thread(() => {
          //val sleeping = round((sending_time*60*1000)/linesMap.apply(fn))
          val dirtyHack = (f:String) => {
            if ((f contains "CapeTownAmsterdam") || (f contains "BuenosAiresSwakopmund")){
              //Эти два маршрута отправим в самом конце, что бы они попали в окно
              Thread.sleep(sending_time*60*1000)
              0
            } else {
              round((sending_time*60*1000)/linesMap.apply(f))
            }
          }
          val sleeping = dirtyHack(fn)
          println(s"Thread started for file $fn, sleeping for $sleeping")
          Thread.currentThread().setName(s"Thread$fn")
          val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)
          val rawData = new File(s"$path_to_csv$fn")
          val reader = rawData.asCsvReader[AisRecord](rfc.withoutHeader)
          try {
            reader.foreach {
              case Left(s) => println(s"Error: $s")
              case Right(i) =>
                val processStart: DateTime = DateTime.now()
                val ntime = processStart.minusSeconds(rand.between(1, 2))
                val inew = i.into[AisRecordTime].withFieldComputed(_.BaseDateTime, _ => dtf.print(ntime)).transform
                val json_ = writeToString(inew);
                producer.send(new ProducerRecord(maintopic, "key", json_))
                  Thread.sleep(sleeping)
            }
        }
        finally
        {
          producer.flush()
          producer.close()
        }

        })
      })
    threads.foreach(t => t.start())
    threads.foreach(t => t.join())
  }
}
