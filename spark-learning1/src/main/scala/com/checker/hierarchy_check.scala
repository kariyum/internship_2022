package com.checker

import com.checker.dimension_check_v2.{getDimensions, getValidDimensions}
import org.apache.spark.sql.functions.{col, collect_list, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.annotation.tailrec
import scala.collection.mutable

object hierarchy_check {
  def getSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  // Not updated
  def hierarchyValidation(dimension : Map[String, mutable.WrappedArray[String]], path : Map[String, String]) : Boolean = {
    // for each key in the path check whether the value in the path map exists in the array of the dimension(key)
    // ! Important, here it is not robust for all path lengths, it is only robust for any path of 2 nodes,
    // we need to verify that the whole linked path is indeed valid.

    // for every key in the path check if the value is in dimension(key) and then go to path(key) as the new key and keep going
    for (path_key <- path.keys) {
      val nextNode = path(path_key)
      val exists = dimension.get(path_key) match {
        case Some(n) => n.contains(nextNode)
        case None => false
      }
      if (!exists) return false
    }
    true
  }
  // this is to check if the values are one string only.
  def hierarchyCheckAPI(path : Map[String, Array[String]]) : Boolean = {
    // check if every value contains a single string
    val semi_valid: Boolean =  {
        val count_arr : Int = path.count(kv => {
          if (kv._2.length > 1) true
          else false
        })
      if (count_arr == 0) true
      else false
    }
    semi_valid
  }
  // Updated.
  def hierarchyCheck(dimension : Map[String, mutable.WrappedArray[String]], path : Map[String, String]) : Boolean = {
    val start : String = path.keys.toSet.diff(path.values.toSet).head // start with the root of the path

    @tailrec
    def rec(subPath : Map[String, String], currNode : String) : Boolean = {
      // check if the subPath(currNode) is in dimension(currNode) if not return false else
      // have the current node as the subPath(currNode) and keep going
      if (subPath.keySet.count(_=>true)==0) return true
      val nextNode = subPath(currNode)
      val exists = dimension.get(currNode) match {
        case Some(n) => n.contains(nextNode)
        case _ => false
      }
      if (!exists) return false
      rec(subPath.filterKeys(key => key != currNode), subPath(currNode))
    }
    rec(path, start)

  }
  // check if the hierarchy is in fact a single path meaning; composed of keys that points to one value
//  def isAHierarchy(path : Map[String, String]) : Boolean = {
//    false
//  }
//  def isAHierarchy(path : Map[String, Array[String]]) : Boolean = {
//    false
//  }
  case class hierarchy_configuration_str(tenant_id : String,
                                     dimension_id : String,
                                     hierarchy_id : String,
                                     from_level : String,
                                     hierarchy_name : String,
                                     to_level : String)
  // Called in data generator V2 object API
  def getHierarchies(hierarchy_path : String, dimension_path : String) : DataFrame = {
    val session = getSession
    session.sparkContext.setLogLevel("WARN")
    import session.implicits._
    val df = session.read.option("header", true)
      .option("delimiter", "|")
      .csv(hierarchy_path)
      .map(row=>{
        hierarchy_configuration_str(
          row.getString(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5)
        )
      })
    // A. Make a new column that maps the from_level to the parent_level
    val dfMapped = df.withColumn("subpath", functions.map(df("from_level"), df("to_level"))).drop("from_level").drop("to_level")
    val joinMap = udf { values: Seq[Map[String, String]] => values.flatten.toMap }

    val dfPath = dfMapped.groupBy(df("tenant_id"), df("dimension_id"), df("hierarchy_id"), df("hierarchy_name")).agg(collect_list("subpath").as("path_array"))
      .withColumn("path", joinMap(col("path_array"))).drop("path_array")

    // preferably get dimension map once for all the dimension_id and pass the necessary map each call... if somehow the dimension map is heavy we would...
    val dimensionMap = getDimensions(dimension_path)
    val validatePath = (dimension_id : String, path : Map[String, String]) =>{
      hierarchyCheck(dimensionMap(dimension_id), path)
    }
    val validatePathUDF = udf(validatePath)
    //    dfPath.withColumn("validity", validatePathUDF(col("dimension_id"), col("path")))

    // either you want to export only valid hierarchies OR
    // you can export the dataframe with the validity column and then filter
    dfPath.filter(validatePathUDF(col("dimension_id"), col("path")))
  }

  def getHierarchies(session : SparkSession): DataFrame ={
    val session = getSession
    session.sparkContext.setLogLevel("WARN")
    import session.implicits._

    val df = session.read.option("header", true)
      .option("delimiter", "|")
      .csv("src/main/resources/ga_input/ga_dimensions_input/hierarchy_configuration.csv")
      .map(row=>{
        hierarchy_configuration_str(
          row.getString(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5)
        )
      })

    // A. Make a new column that maps the from_level to the parent_level
    val dfMapped = df.withColumn("subpath", functions.map(df("from_level"), df("to_level"))).drop("from_level").drop("to_level")
    val joinMap = udf { values: Seq[Map[String, String]] => values.flatten.toMap }

    val dfPath = dfMapped.groupBy(df("tenant_id"), df("dimension_id"), df("hierarchy_id"), df("hierarchy_name")).agg(collect_list("subpath").as("path_array"))
      .withColumn("path", joinMap(col("path_array"))).drop("path_array")

    // preferably get dimension map once for all the dimension_id and pass the necessary map each call... if somehow the dimension map is heavy we would...
    val dimensionMap = getValidDimensions(session)

    val validatePath = (dimension_id : String, path : Map[String, String]) =>{
      hierarchyCheck(dimensionMap(dimension_id), path)
    }
    val validatePathUDF = udf(validatePath)
//    dfPath.withColumn("validity", validatePathUDF(col("dimension_id"), col("path")))

    // either you want to export only valid hierarchies OR
    // you can export the dataframe with the validity column and then filter
    dfPath.filter(validatePathUDF(col("dimension_id"), col("path")))
  }
  case class hierarchy_configuration(tenant_id : Array[Int],
                                     dimension_id : String,
                                     hierarchy_id : String,
                                     from_level : String,
                                     hierarchy_name : String,
                                     to_level : String)

//  def main(args: Array[String]): Unit = {
//    val session = getSession
//    session.sparkContext.setLogLevel("WARN")
//    import session.implicits._
//
//    val df = session.read.option("header", true)
//      .option("delimiter", "|")
//      .csv("src/main/resources/ga_input/ga_dimensions_input/hierarchy_configuration.csv")
//      .map(row=>{
//        hierarchy_configuration(
//          row.getString(0).split(",").map(_.toInt),
//          row.getString(1),
//          row.getString(2),
//          row.getString(3),
//          row.getString(4),
//          row.getString(5)
//        )
//      })
//
//    df.show(false)
//    df.printSchema()
//
//    // A. Make a new column that maps the from_level to the parent_level
//    val dfMapped = df.withColumn("subpath", functions.map(df("from_level"), df("to_level"))).drop("from_level").drop("to_level")
//    dfMapped.show(false)
//    dfMapped.printSchema()
//    val joinMap = udf { values: Seq[Map[String, String]] => values.flatten.toMap }
//
//    val dfPath = dfMapped.groupBy(df("tenant_id"), df("dimension_id"), df("hierarchy_id"), df("hierarchy_name")).agg(collect_list("subpath").as("path_array"))
////      .withColumn("tenant_id", explode(col("tenant_id"))) // we don't need this if we're performing our checks with the withColumn method
//      .withColumn("path", joinMap(col("path_array"))).drop("path_array")
//    dfPath.show(false)
//    dfPath.printSchema()
//
//    // preferably get dimension map once for all the dimension_id and pass the necessary map each call... if somehow the dimension map is heavy we would...
//    val dimensionMap = getValidDimensions(session)
//    if (dimensionMap.keys.count(_=>true)==0){
//      // we should throw an exception here otherwise this would do just as fine.
//      println("Dimension map is empty")
//      return
//    }
//    val validatePath = (dimension_id : String, path : Map[String, String]) =>{
//      hierarchyCheck(dimensionMap(dimension_id), path)
//    }
//    val validatePathUDF = udf(validatePath)
//    val dfValidated = dfPath.withColumn("validity", validatePathUDF(col("dimension_id"), col("path")))
//    dfValidated.show(false)
//  }
}
