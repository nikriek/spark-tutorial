package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_set, lit}
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable

object Sindy {
  val columnName = "col"

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // Read csv data into Dataframes
    // List[String] => List[DataFrame]
    val dataframes = inputs.map(input => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(input)
    })

    import spark.implicits._

    // Cells contains a dataframe of ALL distinct values per column + column name
    val cells = dataframes.map(df => {
      // Get the columns names and use them to select respective columns from the dataframe
      val distinctColumnValues = df.columns
        .map(col => df.select(df(col)).distinct().withColumn(columnName, lit(col)))
      // Merge the distinct (column value, column name)-dataframes
      distinctColumnValues.reduce((df1, df2) => df1.union(df2))
    }).reduce((df1, df2) => df1.union(df2)).as[(String, String)]

    // TODO: Fix wrong col name of attribute
    // TOOD: Look at partitioning

    // Attribute sets contain
    val groupedAttributes = cells.groupBy("R_REGIONKEY").agg(collect_set(columnName))
    val attributeSets = groupedAttributes.select(groupedAttributes("collect_set(col)"))

    // Build inclusion list
    val inclusionLists = attributeSets.flatMap(row => {
      val attributeSet = row.getAs[Seq[String]](0)
      // Generate set e.g [a,b] => [a, [b]], [b, [a]] and filter out the empty ones
      attributeSet.map(attribute => (attribute, attributeSet.filter(_ != attribute)))
        .filter(inclusionList => inclusionList._2.nonEmpty)
    })

    val results = inclusionLists.distinct().take(inclusionLists.count().toInt)

    results.foreach(result => println(s"${result._1} < ${result._2.mkString(",")}"))
  }
}

