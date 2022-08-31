package com.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import spray.json.DefaultJsonProtocol.{StringJsonFormat, arrayFormat, mapFormat}
import spray.json.{JsValue, enrichAny}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Multipart.BodyPart
import com.checker.dimension_check_v2.{checkDimensionMap, validateDimensionsAPI}
import com.checker.hierarchy_check.hierarchyCheckAPI

import scala.concurrent.Future
import scala.io.StdIn
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import com.generator.data_generator_v2.{generateDataAPI, generateDataAPI_fast, generateDataAPI_whole}

import java.nio.file.Files
import scala.concurrent.duration.DurationInt
import scala.reflect.io.File



object http_server {
  /***
   * Converts a JSON Object to a Map
   * @param v : JsValue
   * @return a Map[String, Array[String] ] that has the same values as the input
   */
  def jsonToMap(v : JsValue): Map[String, Array[String]] ={
    val response_map = v.convertTo[Map[String, Array[String]]]
    response_map.foreach(k=>{
      print(k._1 + "->")
      k._2.foreach(v => print(v+", "))
      println()
    })
    response_map
  }

  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    implicit val csvStreaming = EntityStreamingSupport.csv()
    import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._

    val settings = CorsSettings.defaultSettings
    val route = cors(settings) {
      concat(
        path("api" / "dimension_check") {
          get {

            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Why are you here ?</h1>"))
          } ~
            post {
              entity(as[JsValue]) {
                response => {
                  println("JSON Received.");
                  jsonToMap(response)
                  val res: String = if (checkDimensionMap(jsonToMap(response))) "valid." else "not valid."
                  complete("The dimension is " + res)
                  //                  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Post request</h1>"))
                }
              }
            }
        },
        path("api" / "hierarchy_check") {
          post {
            entity(as[JsValue]) {
              response => {
                println("Hierarchy JSON Received.")
                jsonToMap(response)
                val res: Boolean = hierarchyCheckAPI(jsonToMap(response))
                complete(s"Got the hierarchy and it is $res")
              }
            }
          }
        },
        path("api" / "dimension_check_csv") {
          entity(as[Multipart.FormData]) {
            formData => {
              // collect all parts of the multipart as it arrives into a map
              val allPartsF: Future[Map[String, Any]] = formData.parts.mapAsync[(String, Any)](1) {
                case b: BodyPart if b.name == "file" =>
                  // stream into a file as the chunks of it arrives and return
                  // a future file to where it got stored
                  val file = Files.createTempFile("upload", "tmp")
                  println(s"My file $file")
                  b.entity.dataBytes.runWith(FileIO.toPath(file.toRealPath())).map(_ => (b.name -> file))
                case b: BodyPart =>
                  // collect form field values
                  b.toStrict(2.seconds).map(strict =>
                    (b.name -> strict.entity.data.utf8String))
              }.runFold(Map.empty[String, Any])((map, tuple) => map + tuple)

              onSuccess(allPartsF) {
                allParts =>
                  println("File received, responding...")
                  val res: JsValue = validateDimensionsAPI(allParts("file").toString)
//                  println(res.prettyPrint)
//                  println(allParts("file").toString)
                  println("Request fulfilled.")
                  complete(HttpEntity(ContentTypes.`application/json`, res.toString))
                //                  complete("ok!")
              }
            }
          }
        },
        path("api" / "generate_data_hierarchy_csv") {
          entity(as[Multipart.FormData]) {
            formData => {
              val allPartsF: Future[Map[String, Any]] = formData.parts.mapAsync[(String, Any)](1) {
                case b: BodyPart if b.name == "file" =>
                  // stream into a file as the chunks of it arrives and return
                  // a future file to where it got stored
                  val file = Files.createTempFile("upload", ".tmp")
//                  println(s"My file $file")
                  b.entity.dataBytes.runWith(FileIO.toPath(file.toRealPath())).map(_ => (b.name -> file))
                case b: BodyPart =>
                  // collect form field values
                  b.toStrict(2.seconds).map(strict =>
                    (b.name -> strict.entity.data.utf8String))
              }.runFold(Map.empty[String, Any])((map, tuple) => map + tuple)

              onSuccess(allPartsF) {
                allPartsF =>
                  // check if the path exists
                  // else complete with an error
                  println("File received, generating data...")
                  val res: Array[String] = generateDataAPI_whole(allPartsF("file").toString, allPartsF("temp_path").toString)
                  println("Request fulfilled.")
                  complete(HttpEntity(ContentTypes.`text/csv(UTF-8)`, res.mkString("\n")))
              }
            }
          }
        },
        path("api" / "generate_data_hierarchy_print") {
          entity(as[Multipart.FormData]) {
            formData => {
              val allPartsF: Future[Map[String, Any]] = formData.parts.mapAsync[(String, Any)](1) {
                case b: BodyPart if b.name == "file" =>
                  // stream into a file as the chunks of it arrives and return
                  // a future file to where it got stored
                  val file = Files.createTempFile("upload", ".tmp")
                  println(s"My file $file")
                  b.entity.dataBytes.runWith(FileIO.toPath(file.toRealPath())).map(_ => (b.name -> file))
                case b: BodyPart =>
                  // collect form field values
                  b.toStrict(2.seconds).map(strict =>
                    (b.name -> strict.entity.data.utf8String))
              }.runFold(Map.empty[String, Any])((map, tuple) => map + tuple)

              onSuccess(allPartsF) {
                allPartsF =>
                  // check if the path exists
                  // else complete with an error
                  val res: JsValue = generateDataAPI_fast(allPartsF("file").toString, allPartsF("temp_path").toString)
                  println("Request fulfilled.")
                  complete(HttpEntity(ContentTypes.`application/json`, res.toString))
              }
            }
          }
        }
      )
    }

    val production = true
    if (production){
      println("Production")
      Http().bindAndHandle(route, "0.0.0.0", sys.env.getOrElse("HOSTPORT", "5050").toInt)
      println(s"Server online at http://0.0.0.0:${sys.env.getOrElse("HOSTPORT", "5050")}")
    }
    else{
      Http().bindAndHandle(route, "localhost", 5050)
      println(s"Server online at http://localhost:5050/")
    }

  }
}
