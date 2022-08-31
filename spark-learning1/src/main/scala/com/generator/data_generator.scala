package com.generator

import com.checker.hierarchy_check.{getHierarchies, hierarchyValidation}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, explode, udf}

import scala.annotation.tailrec

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Random

object data_generator {
  case class Child(id : String, label : String)
  case class Parent(id : String, label : String)
  // We will use this object to fetch ordered seq of years
  object Year {
    private var counter : Int = getRandomYear.toInt
    def getNext(): String = {
      counter += 1
      counter.toString
    }
  }

  def getSession: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

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
  def getRandomYear : String = (Random.nextInt(25)+1998).toString
  def getAdPeriodFromAdYear(y : String) : Array[String] = {
    val res : Array[String] = (for (idx <- 1.to(13)) yield {
      val s = if (idx < 10) "0"+idx.toString else idx.toString
      y+s
    }).toArray
    res
  }
  def generateColumnsFunction(dimension_id : String, parent_level : String, child_level : String): Map[Parent, Array[Child]] = {
    // dimension_id is needed here to find out the type of the label to generate

    // this function will return a data structure that maps keys -> values
    // for every parent we have many children
    // so we can generate an array of child IDs and an array of child LABELS ==> the return type should be Map[Parent, Array[Child]]

    // we can start by generating the parent id and label

    // we can define how each dimension_id should be treated
    // define a special case for the date parent
    val (parent_id, parent_label) : (String, String) = (dimension_id, parent_level) match {
      case ("time", "Ad Week") => val week = getRandomWeekDay(); (week , week)
      case ("time", "Ad Year") =>  val randomYear = Year.getNext(); (randomYear, randomYear)
      case ("time", "Ad Period") => ("Ad period from case matching_id1", "label from case matching_id1")
      case ("time", "Fiscal Period") => ("Fiscal Period from case matching_id1", "label from case matching_id1")
      case ("time", "Fiscal Week") => ("Fiscal Week from case matching_id1", "label from case matching_id1")
      case ("time", "Fiscal Year") => ("Fiscal Year from case matching_id1", "label from case matching_id1")
      case _ => (parent_level+"_id1", parent_level+"_label_id1")
    }
    val theParent = Parent(parent_id, parent_label)

    // define how many children and then generate their IDs and Labels
    // recursively append a child to an array of children case class
    @tailrec
    def rec(infoArr : Array[String], resArr : Array[Child]) : Array[Child] = {
      if (infoArr.count(_=>true) == 0) return resArr
      val child : Child = Child(infoArr.head, infoArr.head)
      rec(infoArr.tail, resArr ++ Array(child))
    }
//    val firstChild = Child(child_level+"_id1", "label_1")
//    val secondChild = Child(child_level+"_id2", "label_2")
    val res : Array[Child] = (child_level, parent_level) match {
      case ("Day", "Ad Week") => rec(getDaysFromWeek(theParent.id), Array())
      case ("Ad Period", "Ad Year") => rec(getAdPeriodFromAdYear(theParent.id), Array())
      case _ => rec((for (idx <- 1.to(2)) yield child_level+"_id"+ idx.toString).toArray, Array())
    }
    Map(theParent -> res)

  }
//  def main(args: Array[String]): Unit = {
//    val session = getSession
//    import session.implicits._
//    val df = getHierarchies(session)
//    df.show(false)
//    val dfExploaded = df.select(df("tenant_id"), df("dimension_id"), explode(col("path")).as(Seq("child_level", "parent_level"))).cache()
////    val dfExploaded = df.select($"*", explode($"path").as(Seq("from_level", "to_level"))).show(false)
//    dfExploaded.show(false)
//
//    val generateColumns = (dimension_id : String, from_level : String, to_level : String) =>{
//      generateColumnsFunction(dimension_id : String, from_level : String, to_level : String)
//    }
//    val generateColsUDF = udf(generateColumns)
//    // we need to cache the result for a better performance
//    val dfParentToMany = dfExploaded.select($"*", generateColsUDF(col("dimension_id"), col("parent_level"), col("child_level")).as("parent_to_many")).cache()
//
//    // if you there are anymore cache functions that's because i'm printing the dataframe and that's an action,
//    // otherwise we do not need to cache the result since all the transformations gets piled up one after another
//    // and when an action is called spark executes the transformations.
//    val dfParentChildren = dfParentToMany.select($"*", explode(col("parent_to_many"))).drop("parent_to_many")
//
//    val dfParentIdLabelChildren = dfParentChildren.select($"*", col("key.id").as("parent_element_id"), col("key.label").as("parent_element_label")).drop("key").cache()
//    dfParentIdLabelChildren.show(false)
//    val dfChild = dfParentIdLabelChildren.select($"*", explode(col("value")).as("child")).drop("value").cache()
//    dfChild.show(false)
//    val dfDestructured = dfChild.select($"*", col("child.id").as("child_element_id"), col("child.label").as("child_element_label")).drop("child").cache()
//    val columnOrder : Array[String] = "tenant_id|dimension_id|child_level|parent_level|parent_element_id|child_element_id|child_element_label|parent_element_label".split("\\|")
//    val dfOrdered = dfDestructured.select(columnOrder.map(c=>col(c)):_*).cache()
//    dfOrdered.show(100, false)
//    println(dfOrdered.count())
//    dfOrdered.printSchema()
//    dfOrdered.filter(col("dimension_id")==="time" || col("dimension_id")==="calendar").show(50,false)
//  }
}
