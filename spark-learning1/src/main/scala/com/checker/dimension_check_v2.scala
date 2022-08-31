package com.checker

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_list, first, udf}
import spray.json.DefaultJsonProtocol.{JsValueFormat, StringJsonFormat, arrayFormat, mapFormat}
import spray.json.{JsValue, enrichAny}

import scala.annotation.tailrec
import scala.collection.mutable

import spray.json._

object dimension_check_v2 {
  def getSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  def getDfGroupedOnDimensionLvl(session : SparkSession): DataFrame = {
    import session.implicits._
    val df = session.read.option("header", true).option("delimiter", "|").csv("src/main/resources/ga_input/ga_dimensions_input/dimension_configuration.csv")
      .map(e => dimension_configuration(e.getString(0).split(",").map(_.toInt),
                                        e.getString(1),
                                        e.getString(2),
                                        e.getString(3).split(",").map(_.trim)))

    val dfGroupedOnDimensionLvl = df.withColumn("mymap", functions.map(col(df.columns(2)), col(df.columns(3)))).drop(df.columns(2)).drop(df.columns(3))

    // df grouped on dimension id such as (time, product, ...)
    val joinMap = udf { values: Seq[Map[String, Array[String]]] => values.flatten.toMap }
    dfGroupedOnDimensionLvl.groupBy(df.columns(1)).agg(first(df.columns(0)) as df.columns(0), collect_list("mymap").as("mymap")).withColumn("mymap", joinMap(col("mymap")))
  }

  def getDimension(session : SparkSession): Map[String, Map[String, mutable.WrappedArray[String]]] ={
    val dff = getDfGroupedOnDimensionLvl(session)
    dff.show(false)
    println("JSON")
    val list : Array[JsValue] = dff.withColumnRenamed("mymap", "value").toJSON.collect().map(s => s.parseJson)
//    list.foreach(s => println(s.prettyPrint))
//    list.foreach(s=>println(s.prettyPrint))
//    println(list.toJson.prettyPrint)
    println(Map("payload"->list).toJson.prettyPrint)
    val dimensionIdMappedToGraph : Map[String, Map[String, mutable.WrappedArray[String]]] = dff.select("dimension_id", "mymap").collect().map(k=>{
      val entry = k.getString(0)-> k.getAs[Map[String, mutable.WrappedArray[String]]](1)
      entry
    }).toMap
    dimensionIdMappedToGraph
  }

  // this is used by the hierarchy check API function
  def getDimensions(path : String) : Map[String, Map[String, mutable.WrappedArray[String]]] = {
    val session = getSession
    session.sparkContext.setLogLevel("WARN")

    import session.implicits._
    val df = session.read.option("header", true).option("delimiter", "|").csv(path)
      .map(e => dimension_configuration(e.getString(0).split(",").map(_.toInt),
        e.getString(1),
        e.getString(2),
        e.getString(3).split(",").map(_.trim)))

    val dfGroupedOnDimensionLvl = df.withColumn("mymap", functions.map(col(df.columns(2)), col(df.columns(3)))).drop(df.columns(2)).drop(df.columns(3))

    // df grouped on dimension id such as (time, product, ...)
    val joinMap = udf { values: Seq[Map[String, Array[String]]] => values.flatten.toMap }
    val dfGrouped = dfGroupedOnDimensionLvl.groupBy(df.columns(1)).agg(first(df.columns(0)) as df.columns(0), collect_list("mymap").as("mymap")).withColumn("mymap", joinMap(col("mymap")))

//    dfGrouped.show(false)

    val validate = ( map : Map[String, mutable.WrappedArray[String]]) => {
      rec(map)
    }
    val validateUDF = udf{validate}

    val dfVerified = dfGrouped.withColumn("validity", validateUDF(col("mymap")))
    val dimensionIdMappedToGraph : Map[String, Map[String, mutable.WrappedArray[String]]]= dfVerified.select("dimension_id", "mymap").collect().map(k=>{
      val entry = k.getString(0)-> k.getAs[Map[String, mutable.WrappedArray[String]]](1)
      entry
    }).toMap
    dimensionIdMappedToGraph
  }

  def validateDimensionsAPI(path : String) : JsValue = {
    val session = getSession
    session.sparkContext.setLogLevel("WARN")

    import session.implicits._
    val df = session.read.option("header", true).option("delimiter", "|").csv(path)
      .map(e => dimension_configuration(e.getString(0).split(",").map(_.toInt),
        e.getString(1),
        e.getString(2),
        e.getString(3).split(",").map(_.trim)))

    val dfGroupedOnDimensionLvl = df.withColumn("mymap", functions.map(col(df.columns(2)), col(df.columns(3)))).drop(df.columns(2)).drop(df.columns(3))

    // df grouped on dimension id such as (time, product, ...)
    val joinMap = udf { values: Seq[Map[String, Array[String]]] => values.flatten.toMap }
    val dfGrouped = dfGroupedOnDimensionLvl.groupBy(df.columns(1)).agg(first(df.columns(0)) as df.columns(0), collect_list("mymap").as("mymap")).withColumn("mymap", joinMap(col("mymap")))

//    dfGrouped.show(false)

    val validate = ( map : Map[String, mutable.WrappedArray[String]]) => {
      rec(map)
    }
    val validateUDF = udf{validate}

    val dfVerified = dfGrouped.withColumn("validity", validateUDF(col("mymap")))

    val json_arr = dfVerified.toJSON.collect().map(s => s.parseJson)

    val json_arr_path : Array[JsValue] = Array(path.toJson)
    Map("payload"->json_arr, "temp_path"->json_arr_path).toJson
//    "{}".parseJson
  }

  def getValidDimensions(session: SparkSession): Map[String, Map[String, mutable.WrappedArray[String]]] = {
    val dff = getDfGroupedOnDimensionLvl(session)
    val validateDimension = (graph: Map[String, mutable.WrappedArray[String]]) => {
      rec(graph)
    }
    val validateDimensionUDF = udf {
      validateDimension
    }

    val res = dff.withColumn("validity", validateDimensionUDF(col("mymap"))).filter(col("validity") === true)
    val dimensionIdMappedToGraph : Map[String, Map[String, mutable.WrappedArray[String]]]= res.select("dimension_id", "mymap").collect().map(k=>{
      val entry = k.getString(0)-> k.getAs[Map[String, mutable.WrappedArray[String]]](1)
      entry
    }).toMap
    dimensionIdMappedToGraph
  }

  @tailrec
  def rec(argMap : Map[String, mutable.WrappedArray[String]]) : Boolean = {
    // 1. extract a set of parent_level Strings that aren't present in the set of keys
    // 2. remove the keys from the map
    // 3. loop until there aren't any keys left -> returns true VALID
    //    or until we can't delete anymore keys -> return false CYCLE DETECTED

    // 1.
    // a. Get the set of parent_level
    val parentLvlSet = argMap.values.flatten.toSet

    // b. Get the set of keys
    val dimensionLvlSet = argMap.keySet

    // c. Compute the difference
    val toGoSet = parentLvlSet.diff(dimensionLvlSet).map(element => {
      element
    }).toArray

    // 2. Remove those values from the m(dimension_id) values
    val newMap = argMap.mapValues(e =>{
      e.diff(toGoSet)
    })

    // 2.bis. Remove keys that points to a empty array
    val updatedSet = newMap.filterKeys(key=>{
      newMap(key).count(_=>true) != 0
    })

    // 3. loop or return
    val oldSetCount = parentLvlSet.count(_=>true)
    val newSetCount = updatedSet.values.flatten.toSet.count(_=>true)

    if (newSetCount == oldSetCount) {
      return false
    }
    if (newSetCount == 0) return true

    rec(updatedSet)
  }

  def checkDimensionMap(dimension : Map[String, Array[String]]) : Boolean = {
    @tailrec
    def check(argMap : Map[String, Array[String]]) : Boolean = {
      // 1. extract a set of parent_level Strings that aren't present in the set of keys
      // 2. remove the keys from the map
      // 3. loop until there aren't any keys left -> returns true VALID
      //    or until we can't delete anymore keys -> return false CYCLE DETECTED

      // 1.
      // a. Get the set of parent_level
      val parentLvlSet = argMap.values.flatten.toSet

      // b. Get the set of keys
      val dimensionLvlSet = argMap.keySet

      // c. Compute the difference
      val toGoSet = parentLvlSet.diff(dimensionLvlSet).map(element => {
        element
      }).toArray

      // 2. Remove those values from the m(dimension_id) values
      val newMap = argMap.mapValues(e =>{
        e.diff(toGoSet)
      })

      // 2.bis. Remove keys that points to a empty array
      val updatedSet = newMap.filterKeys(key=>{
        newMap(key).count(_=>true) != 0
      })

      // 3. loop or return
      val oldSetCount = parentLvlSet.count(_=>true)
      val newSetCount = updatedSet.values.flatten.toSet.count(_=>true)

      if (newSetCount == oldSetCount) {
        return false
      }
      if (newSetCount == 0) return true

      check(updatedSet)
    }
    check(dimension)
  }

  case class dimension_configuration(tenant_id : Array[Int], dimension_id : String, dimension_level : String, parents_level : Array[String])

//  def main(args: Array[String]): Unit = {
//    val session = getSession
//    session.sparkContext.setLogLevel("WARN")
//    val dimensionIdMappedToGraph = getDimension(session)
//    // for every dimension_id we're going to verify whether the graph == dimensionIdMappedToGraph(dimension_id) is acyclic or not
//    for (dimension_id <- dimensionIdMappedToGraph.keys) {
//      val subMap = dimensionIdMappedToGraph(dimension_id)
//      val valid = rec(subMap)
//      println(dimension_id + " dimension validity is " + valid)
//    }
//    getValidDimensions(session)
//  }
}
