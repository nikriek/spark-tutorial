package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import  org.apache.spark.sql.functions.{lit}
object Sindy {

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

    // Cells contains a dataframe of ALL distinct values per column + column name
    val cells = dataframes.map(df => {
      // Get the columns names and use them to select respective columns from the dataframe
      val distinctColumnValues = df.columns.map(col => df.select(df(col)).distinct().withColumn("column_name", lit(col)))
      // Merge the distinct (column value, column name)-dataframes
      distinctColumnValues.reduce((df1, df2) => df1.union(df2))
    }).reduce((df1, df2) => df1.union(df2))


  }
}

