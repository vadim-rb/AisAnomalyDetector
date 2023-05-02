package utils

object Config {
  val routes_csv: List[String] = List(
    "BridgetownPortsmouth.csv",
    "GuantaNatal.csv",
    "Pointe-a-PitreHavre.csv",
    "BuenosAiresSwakopmund.csv",
    "MiamiLisbon.csv",
    "SanJuanBilbao.csv",
    "CapeTownAmsterdam.csv",
    "NewYorkGalway.csv",
    "CharlestonCardiff.csv",
    "NorfolkTanji.csv"
  )
////Norfolk-Tanji 146061087 Guanta-Natal 171443472
  val routes_csv_1: List[String] = List(
    "GuantaNatal.csv",
    "NorfolkTanji.csv"
  )
  //Buenos Aires(Argentina) -> Swakopmund(Namibia) 130784060
  //Cape Town(South Africa) -> Amsterdam(Netherlands) 169066065
  val routes_csv_2: List[String] = List(
    "BuenosAiresSwakopmund.csv",
    "CapeTownAmsterdam.csv"
  )

  val routes_csv_3: List[String] = List(
    "BuenosAiresSwakopmund.csv",
    "CapeTownAmsterdam.csv",
    "GuantaNatal.csv",
    "NorfolkTanji.csv"
  )

  val path_to_csv = "/home/vadim/MyExp/Diplom/IdeaProjects/dtest/D/src/main/resources/transformations/"
  val bootstrap_servers =  "localhost:29092"
  val sending_time = 3 //minutes
  val maintopic = "ais"
  val Distance = 70 //meters
  val Window_time = 2 //minutes

  val linesMap: Map[String, Int] = Map(
    "BridgetownPortsmouth.csv"->4362,
    "NorfolkTanji.csv" -> 5509,
    "GuantaNatal.csv" -> 3101,
    "SanJuanBilbao.csv" ->3834,
    "BuenosAiresSwakopmund.csv" ->5569,
    "MiamiLisbon.csv"->5249,
    "Pointe-a-PitreHavre.csv" ->3289,
    "CapeTownAmsterdam.csv" ->10235,
    "CharlestonCardiff.csv" ->4658,
    "NewYorkGalway.csv"->3307
  )

  val postgresqlSinkOptionsAis: Map[String, String] = Map(
    "dbtable" -> "public.ais_data", // table
    "user" -> "postgres", // Database username
    "password" -> "mysecretpassword", // Password
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5455/postgres"
  )

  val postgresqlSinkOptionsCatched: Map[String, String] = Map(
    "dbtable" -> "public.catched", // table
    "user" -> "postgres", // Database username
    "password" -> "mysecretpassword", // Password
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5455/postgres"
  )

}
