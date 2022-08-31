package com.checker

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object dimension_check {
//  def main(args: Array[String]): Unit = {
//    val session = getSession()
//    session.sparkContext.setLogLevel("WARN")
//    // == Load dimension hierarchy and validate it == //
//    import session.implicits._
//    // import session.sqlContext.implicits._
//    // loading dimension from resources/ga_input/ga_dimension_input/dimension_configuration in a RDD format (questionable)
//    // val dcFromFile = session.sparkContext.textFile("src/main/resources/ga_input/ga_dimensions_input/dimension_configuration.csv")
//    // val dc = dcFromFile.map(f => f.split('|'))
//    // dc contains Array[Array[String]]
//    val ds = session.read.option("header", true)
//      .option("delimiter", "|")
//      .csv("src/main/resources/ga_input/ga_dimensions_input/dimension_configuration.csv")
//      .map(e => dimension_configuration(e.getString(0).split(",").map(_.toInt), e.getString(1), e.getString(2), e.getString(3).split(",").map(_.trim)))
//    ds.printSchema()
//    ds.show(50, false)
//
//    //    val table_name = "dimension_configuration"
//    val ds1 = ds.withColumn("parent_level", explode(col("parents_level"))).drop("parents_level").withColumn("tenant_id1", explode(col("tenant_id"))).drop("tenant_id")
//    ds1.printSchema()
//    ds1.show(false)
//    //    val time_table = session.sql(s"select * from $table_name where $table_name.dimension_id='time'")
//    println(checkConfiguration(ds1.filter(ds1.col("dimension_id") === "product" && ds1.col("tenant_id1") === 1), session))
//  }

  def getSession() = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()
  case class dimension_configuration(tenant_id : Array[Int], dimension_id : String, dimension_level : String, parents_level : Array[String])
  def checkConfiguration(ds : Dataset[Row], session : SparkSession): Boolean = {
    // delete lines where the parents_level isn't in the dimension_level column
    // repeat until there are no lines or until you can't delete anymore => that's a cycle
    def rec(dsAux : Dataset[Row], session : SparkSession, previousCount : Long) : Boolean = {
      if (dsAux.count() == 0) {
        true
      } else {
        dsAux.createOrReplaceTempView("table")
        var ds1 = session.sql("select * from table MINUS (select * from table where table.parent_level not in (select table.dimension_level from table))")
        if (ds1.count() == previousCount) return false
        rec(ds1, session, ds1.count())
      }
    }
    rec(ds, session, ds.count())
  }
}
