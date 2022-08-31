package com.generator

import com.checker.hierarchy_check.getHierarchies
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, map_concat, udf}
import spray.json.DefaultJsonProtocol.mapFormat
import spray.json.JsValue
import spray.json._
import spray.json.DefaultJsonProtocol.{JsValueFormat, StringJsonFormat, arrayFormat, mapFormat}
import spray.json.{JsValue, enrichAny}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random

object data_generator_v2 {

  /***
   * Called by http_server in order to generate data from a given hierarchy file path and the dimension path
   * @param hierarchy_path
   * @param dimension_path
   * @return Array[String] contains CSV strings (rows)
   */
  // V1 -- Sends one whole dataframe
  def generateDataAPI_whole(hierarchy_path : String, dimension_path : String) : Array[String] = {
    val session = getSession
    import session.implicits._
    println("Loading and verifying hierarchies...")
    val df : DataFrame = getHierarchies(hierarchy_path, dimension_path)

    val generateData = (hierarchy : Map[String, String]) =>{
      generateDataFn(getParentToChildPath(hierarchy))
    }

    val generateDataUDF = udf(generateData)
    val dfParentToChild = df.select($"*", generateDataUDF(col("path")).as("path_data"))
    println("Data generated.")

    val dfLast2 = dfParentToChild.select($"*", explode(col("path_data")).as(Seq("parent_element", "value"))).drop("path_data")
    val dfLast3 = dfLast2.select($"*", explode(col("value")).as("child_element")).drop(col("value"))
    val dfDestructured = dfLast3.select( $"*",
      col("parent_element").getField("id").as("parent_element_id"),
      col("parent_element").getField("label").as("parent_element_label"),
      col("parent_element").getField("level").as("parent_level"),
      col("child_element").getField("id").as("child_element_id"),
      col("child_element").getField("label").as("child_element_label"),
      col("child_element").getField("level").as("child_level"))
      .drop("parent_element", "child_element").cache()
    val columnOrder : Array[String] = "tenant_id|hierarchy_id|hierarchy_name|dimension_id|child_level|parent_level|parent_element_id|child_element_id|child_element_label|parent_element_label".split("\\|")
    val dfOrdered = dfDestructured.select(columnOrder.map(c=>col(c)):_*).cache()

    Array(columnOrder.mkString(",")) ++ dfOrdered.collect().map( row=> row.toSeq.map(c=>c.toString).mkString(","))
  }

  // v2 -- Grouped
  def generateDataAPI(hierarchy_path : String, dimension_path : String) : JsValue = {
    val session = getSession
    import session.implicits._

    val df : DataFrame = getHierarchies(hierarchy_path, dimension_path)
    df.show(false)

    val generateData = (hierarchy : Map[String, String]) =>{
      generateDataFn(getParentToChildPath(hierarchy))
    }

    val generateDataUDF = udf(generateData)
    val dfParentToChild = df.select($"*", generateDataUDF(col("path")).as("path_data"))

//    dfParentToChild.show(false)

    val dataframe_arr : Array[Array[JsValue]] = dfParentToChild.collect().map(row => {
      val aux : DataFrame = dfParentToChild.filter( col("hierarchy_id") === row.getAs("hierarchy_id") && col("tenant_id") === row.getAs("tenant_id"))
      aux.show(false)
      val dfLast2 = aux.select($"*", explode(col("path_data")).as(Seq("parent_element", "value"))).drop("path_data")
      val dfLast3 = dfLast2.select($"*", explode(col("value")).as("child_element")).drop(col("value"))
      val dfDestructured = dfLast3.select( $"*",
        col("parent_element").getField("id").as("parent_element_id"),
        col("parent_element").getField("label").as("parent_element_label"),
        col("parent_element").getField("level").as("parent_level"),
        col("child_element").getField("id").as("child_element_id"),
        col("child_element").getField("label").as("child_element_label"),
        col("child_element").getField("level").as("child_level"))
        .drop("parent_element", "child_element").cache()
      val columnOrder : Array[String] = "tenant_id|hierarchy_id|hierarchy_name|dimension_id|child_level|parent_level|parent_element_id|child_element_id|child_element_label|parent_element_label|path".split("\\|")
      val dfOrdered = dfDestructured.select(columnOrder.map(c=>col(c)):_*).cache()
//      dfOrdered.show(false)
      dfOrdered.toJSON.collect().map(s=>s.parseJson)
    })


    val json_arr : Array[Array[JsValue]] = dataframe_arr
    Map("payload"->json_arr).toJson
  }

  //df grouped but with futures
  def generateDataAPI_fast(hierarchy_path : String, dimension_path : String) : JsValue = {
    val session = getSession
    import session.implicits._
    println("Loading and verifying hierarchies...")
    val df : DataFrame = getHierarchies(hierarchy_path, dimension_path)

    println("Generating data...")
    val generateData = (hierarchy : Map[String, String]) =>{
      generateDataFn(getParentToChildPath(hierarchy))
    }

    val generateDataUDF = udf(generateData)
    val dfParentToChild = df.select($"*", generateDataUDF(col("path")).as("path_data"))

//    dfParentToChild.show(false)

    // Array[JsValue] holds the rows as JSON


    println("Data generated, grouping by hierarchies")
    val dataframe_arr : Seq[Future[Array[JsValue]]] = dfParentToChild.collect().map(row => {
      val aux : DataFrame = dfParentToChild.filter( col("hierarchy_id") === row.getAs("hierarchy_id") && col("tenant_id") === row.getAs("tenant_id"))
//      println("showing aux dataframe")
//      aux.show(false)
      val dfLast2 = aux.select($"*", explode(col("path_data")).as(Seq("parent_element", "value"))).drop("path_data")
      val dfLast3 = dfLast2.select($"*", explode(col("value")).as("child_element")).drop(col("value"))
      val dfDestructured = dfLast3.select( $"*",
        col("parent_element").getField("id").as("parent_element_id"),
        col("parent_element").getField("label").as("parent_element_label"),
        col("parent_element").getField("level").as("parent_level"),
        col("child_element").getField("id").as("child_element_id"),
        col("child_element").getField("label").as("child_element_label"),
        col("child_element").getField("level").as("child_level"))
        .drop("parent_element", "child_element")
      val columnOrder : Array[String] = "tenant_id|hierarchy_id|hierarchy_name|dimension_id|child_level|parent_level|parent_element_id|child_element_id|child_element_label|parent_element_label|path".split("\\|")
      val dfOrdered = dfDestructured.select(columnOrder.map(c=>col(c)):_*)
//      dfOrdered.show(false)
      Future {
        dfOrdered.toJSON.collect().map(s=>s.parseJson)
      }
    }).toSeq
    println("Waiting for futures to resolve...")
    val json_arr : Array[Array[JsValue]] = Await.result(Future.sequence(dataframe_arr), 60.seconds).toArray
    Map("payload"->json_arr).toJson
  }

//  def main(args: Array[String]): Unit = {
//    val session = getSession
//    import session.implicits._
//    val df : DataFrame = getHierarchies(session)//.filter(row=>List("fiscal_hierarchy", "fiscal_quarter_hierarchy", "fiscal_timeframe_hierarchy").contains(row.getString(2)))
////    df.show(false)
//
//    // making sure that the path is correctly inverted
////    val invertPathFN = (path : Map[String, String]) => {
////      getParentToChildPath(path)
////    }
////    val invertPathUDF = udf(invertPathFN)
////    df.select(invertPathUDF(col("path"))).show(false)
//    // end.
//
//    val generateData = (hierarchy : Map[String, String]) =>{
//      generateDataFn(getParentToChildPath(hierarchy))
//    }
//
//    val generateDataUDF = udf(generateData)
//    val dfParentToChild = df.select($"*", generateDataUDF(col("path")).as("path_data")).drop("path")
//
//    dfParentToChild.show(false)
//
//    val dfLast2 = dfParentToChild.select($"*", explode(col("path_data")).as(Seq("parent_element", "value"))).drop("path_data")
//    val dfLast3 = dfLast2.select($"*", explode(col("value")).as("child_element")).drop(col("value"))
//    val dfDestructured = dfLast3.select( $"*",
//      col("parent_element").getField("id").as("parent_element_id"),
//      col("parent_element").getField("label").as("parent_element_label"),
//      col("parent_element").getField("level").as("parent_level"),
//      col("child_element").getField("id").as("child_element_id"),
//      col("child_element").getField("label").as("child_element_label"),
//      col("child_element").getField("level").as("child_level"))
//      .drop("parent_element", "child_element").cache()
//    val columnOrder : Array[String] = "tenant_id|hierarchy_id|hierarchy_name|dimension_id|child_level|parent_level|parent_element_id|child_element_id|child_element_label|parent_element_label".split("\\|")
//    val dfOrdered = dfDestructured.select(columnOrder.map(c=>col(c)):_*).cache()
////    dfOrdered.show(1200, false)
////    println(dfOrdered.count())
////    dfOrdered.filter(col("hierarchy_id") === "fiscal_timeframe_hierarchy").show(false)
//
////    dfOrdered
////      .coalesce(1)
////      .write
////      .format("csv")
////      .option("header", "true")
////      .option("delimiter", "|")
////      .mode(SaveMode.Overwrite)
////      .save("C:/Users/Intern/Desktop/test")
//
//    //    dfOrdered.coalesce(1)
////      .write
////      .option("header", "true")
////      .option("delimiter", "\t")
////      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
////      .mode(SaveMode.Append)
////      .csv("./tmp")
////    write.format('csv').option('header',True).mode('overwrite').option('sep','|').save('/output.csv')
////      .coalesce(1)
////      .write.format("csv")
////      .mode(SaveMode.Overwrite)
////      .save("c:\\tmp\\spark_output")
//  }

  case class Node(id : String, label : String, level: String)

  def getSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  def getRandomYear : String = (Random.nextInt(25)+1998).toString

  def getAdPeriodFromAdYear(y : String) : Array[String] = {
    val res : Array[String] = (for (idx <- 1.to(13)) yield {
      val s = if (idx < 10) "0"+idx.toString else idx.toString
      y+s
    }).toArray
    res
  }

  def getRandomWeekDay(fDay : String = "WEDNESDAY") : String = {
    val firstDay = fDay.map(c=>c.toUpper)
    val date = LocalDateTime.now().minusYears(Random.nextInt(25)).minusWeeks(Random.nextInt(25)).minusMonths(Random.nextInt(25))
    val days = (1.to(7)).map(i=>date.plusDays(i))
    val startDate = for (e <- days if e.getDayOfWeek.toString == firstDay) yield e
    startDate(0).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

  }

  def getDaysFromWeek(weekStart : String) : Array[String] = {
    val ymd = weekStart.split("-").map(f => f.toInt)
    val date = LocalDateTime.of(ymd(0), ymd(1), ymd(2), 0, 0)
    val days : Array[String] = (for (i <- 0.to(6)) yield date.plusDays(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).toArray
    days
  }

  def getWeekFromAdPeriod(adPeriod : String) : Array[String] = {
    val year = adPeriod.substring(0, 4).toInt
    val date = LocalDateTime.of(year, 1, 1, 0, 0)
    val period = adPeriod.substring(4,6).toInt
    def recWeek(times : Int, date : LocalDateTime) : Array[String] = {
      if (times==1) return (for (i <- 0.to(3)) yield date.plusWeeks(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).toArray
      recWeek(times-1, date.plusWeeks(4))
    }
    recWeek(period, date)
  }

  def getWeekIndexFromPeriod(period : String) : Array[String] = {
    val year = period.substring(0, 4).toInt
    val p = period.substring(4,6)
    1.to(4).map(idx=>year.toString+p+"0"+(idx).toString).toArray
  }

  def getWeekFromIndex(weekIdx : String) : Array[String] = {
    val year = weekIdx.substring(0, 4).toInt
    val date = LocalDateTime.of(year, 1, 1, 0, 0)
    val p = weekIdx.substring(4,6).toInt
    val idx = weekIdx.substring(6,8).toInt
    def recWeek(times : Int, date : LocalDateTime) : Array[String] = {
      if (times==1) return (for (i <- 0.to(3)) yield date.plusWeeks(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).toArray
      recWeek(times-1, date.plusWeeks(4))
    }
    Array(recWeek(p, date).apply(idx-1))

  }

  def getFiscalQuarter : String = {
    getRandomYear + "0" + (Random.nextInt(4)+1).toString
  }

  def getPeriodFromQuarter(quarter : String) : Array[String] = {
    val year = quarter.substring(0, 4)
    val q = quarter.substring(4, 6).toInt
    getAdPeriodFromAdYear(year).slice(3*(q-1), 3*(q-1)+4)
  }

  @tailrec
  def rec(infoArr : Array[String], child_level : String, resArr : Array[Node] = Array()) : Array[Node] = {
    // this is for generating child objects
    if (infoArr.count(_=>true) == 0) return resArr
    val stringIdx = (resArr.count(_=>true)+1).toString

    val yearPattern = """(^(Fiscal|Ad) Year$)""".r
    val weekPattern = """(^(Fiscal|Ad) Week$)""".r
    val periodPattern = """(^(Fiscal|Ad) Period$)""".r

    val n : Node = (child_level) match {
      case periodPattern(child_level, _) => Node(infoArr.head, infoArr.head, child_level)
      case yearPattern(child_level, _) => Node(infoArr.head, infoArr.head, child_level)
      case weekPattern(child_level, _) => Node(infoArr.head, infoArr.head, child_level)
      case "Fiscal Week Index" => Node(infoArr.head, infoArr.head, child_level)
      case "Day" => Node(infoArr.head, infoArr.head, child_level)
      case _ => Node(infoArr.head+"_id"+stringIdx, infoArr.head+"_label"+stringIdx, child_level)
    }
    rec(infoArr.tail, child_level, resArr ++ Array(n))
  }

  def generateChildren(child_level : String, parent : Node) : Array[Node] = {
    // generate an array of Node from parent_level, do we need the dimension_id ?
    if (child_level == "") return Array()
    // we're going to use the label of the parent to generate the children

    val res = (child_level, parent.level) match {
      // make sure that the parent_level is generated accordingly, because it is passed as an argument that would generate the data with respect to the parent_label
      case ("Ad Period", "Ad Year") => rec(getAdPeriodFromAdYear(parent.label), child_level)
      case ("Ad Week", "Ad Period") => rec(getWeekFromAdPeriod(parent.label), child_level)
      case ("Day", "Ad Week") => rec(getDaysFromWeek(parent.label), child_level)
      case ("Fiscal Period", "Fiscal Year") => rec(getAdPeriodFromAdYear(parent.label), child_level)
      case ("Fiscal Week Index", "Fiscal Period") => rec(getWeekIndexFromPeriod(parent.label), child_level)
      case ("Fiscal Week", "Fiscal Week Index") => rec(getWeekFromIndex(parent.label), child_level)
      case ("Fiscal Period", "Fiscal Quarter") => rec(getPeriodFromQuarter(parent.label), child_level)
      case ("Fiscal Week", "Fiscal Period") => rec(getWeekFromAdPeriod(parent.label), child_level)
      case ("Day", "Fiscal Week") => rec(getDaysFromWeek(parent.label), child_level)
      case _ => rec(1.to(Random.nextInt(2)+3).map(c=>child_level).toArray, child_level, Array())
    }
    res
  }

  def generateParent(level : String) : Node = {
    val yearPattern = """(^(Fiscal|Ad) Year$)""".r
    val weekPattern = """(^(Fiscal|Ad) Week$)""".r
    val periodPattern = """(^(Fiscal|Ad) Period$)""".r
    level match {
      case yearPattern(level, _) => val year = getRandomYear; Node(year, year, level)
      case weekPattern(level, _) => val week = getRandomWeekDay(); Node(week, week, level)
      case periodPattern(level, _) => val period = getAdPeriodFromAdYear(getRandomYear)(Random.nextInt(13)); Node(period, period, level)
      case "Fiscal Quarter" => val quarter = getFiscalQuarter; Node(quarter, quarter, level)
      case _ => Node(level+"_id", level+"_label", level)
    }
  }


  def recChild(parent : Node, path : Map[String, String], accumulatedMapRes : Map[Node, Array[Node]] ): Map[Node, Array[Node]] = {
    val newPath : Map[String, String] = path.filterKeys(_!=parent.level)
    val children : Array[Node] = generateChildren(path.get(parent.level) match {
      case Some(n) => n
      case _ => ""
    }, parent)
    val res : Map[Node, Array[Node]] = children.flatMap(child=>{
      recChild(child, newPath, accumulatedMapRes)
    }).toMap
    if (children.count(_=>true)==0) return res
    Array(res, Map(parent->children)).flatten.toMap
  }

  // returns the next path without the entry where the root is the value
  def getNextPath(path : Map[String, String]) : Map[String, String] = {
    path.filterKeys(key=>path(key)!=getRoot(path))
  }
  // get root parent returns the parent of the root
  def getRootParent(path : Map[String, String]) : String = {
    path.filter(pair => pair._2 == getRoot(path)).keys.head
  }

  // Invert the map
  def getParentToChildPath(path : Map[String, String]) : Map[String, String] = {
    @tailrec
    def rec(subPath : Map[String, String], res : Map[String, String]) : Map[String, String] = {
      if (subPath.keys.count(_=>true)==0) return res
      rec(getNextPath(subPath), Array(res, Map(getRoot(subPath)-> getRootParent(subPath))).flatten.toMap)
    }
    rec(path, Map())
  }

  // getRoot returns the root of the path, root points to nothing.
  def getRoot(path : Map[String, String]) : String = {
    path.values.toSet.diff(path.keySet).head
  }

  def generateDataFn(path : Map[String, String]): Map[Node, Array[Node]] = {
    // find out which node is parent only. We're going to have one instance of that node let's call it the QUEEN
    // starting from the queen we're going to generate children for each key
    val parentLevelNode = generateParent(path.keySet.diff(path.values.toSet).head)
    recChild(parentLevelNode, path, Map())
    // Map(Parent("id1", "label1", "level1")-> Array(Child("id1", "label1", "level1"), Child("id2", "label2", "level2")))
  }
}
