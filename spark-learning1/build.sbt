name := "spark-learning1"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
val AkkaVersion = "2.5.21"
val AkkaHttpVersion = "10.1.15"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % "2.5.31",
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.15",
  "ch.megard" %% "akka-http-cors" % "0.4.2"
)


enablePlugins(PackPlugin)
packMain := Map("main" -> "com.routes.http_server")