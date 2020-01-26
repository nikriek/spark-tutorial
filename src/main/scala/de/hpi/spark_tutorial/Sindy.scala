package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_set, lit}
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable

object Sindy {
  val columnName = "attribute"

  type AttributeSet = Seq[String]
  type Attribute = String

  case class Inclusion(attribute: Attribute,
                       attributeSet: AttributeSet)

  case class InclusionList(attribute: Attribute,
                           attributeSet: Seq[AttributeSet])

  case class Value(value: String,
                   attribute: Attribute)

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // TODO: Look at partitioning

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

    // Cells contains a dataframe of ALL distinct values per column + attribute name
    val cells = dataframes.map(df => {
      // Get the columns names and use them to select respective columns from the dataframe
      val distinctColumnValues = df.columns
        .map(col => df.select(df(col)).distinct().withColumn(columnName, lit(col)))
      // Merge the distinct (column value, column name)-dataframes
      distinctColumnValues.reduce((df1, df2) => df1.union(df2))
    }).reduce((df1, df2) => df1.union(df2))

    // Attribute sets are grouping together attributes that have the same value in the column
    val groupedAttributes = cells
      .groupBy(cells.columns(0))
      .agg(collect_set(cells.columns(1)))

    val attributeSets = groupedAttributes.select(groupedAttributes.columns(1)).as[AttributeSet]

    // Build inclusion list from attribute sets
    val inclusionLists = attributeSets.flatMap(inclusion => {
        // Generate set e.g [a,b] => [a, [b]], [b, [a]]
        inclusion.map(attribute => (attribute, inclusion.filter(_ != attribute)))
      }).filter(inclusion => inclusion._2.nonEmpty)
      .toDF("attribute", "attributeSet")
        .as[Inclusion]

    // Build result list by collecting attribute sets and
    // merging them using the intersection
    val results = inclusionLists
      .groupBy(inclusionLists.columns(0))
      .agg(collect_set(inclusionLists.columns(1)))
      .toDF("attribute", "attributeSet")
      .as[InclusionList]
      .map(inclusionList => (inclusionList.attribute, inclusionList.attributeSet.flatten.distinct))
      .toDF("attribute", "attributeSet")
      .orderBy(org.apache.spark.sql.functions.col("attribute").asc)
      .as[Inclusion]


    // Print results in desired format
    results.foreach(result => println(s"${result.attribute} < ${result.attributeSet.mkString(",")}"))

    /*
    C_CUSTKEY < P_PARTKEY
    C_NATIONKEY < S_NATIONKEY, N_NATIONKEY
    L_COMMIT < L_SHIP, L_RECEIPT
    L_LINENUMBER < C_NATIONKEY, S_NATIONKEY, O_ORDERKEY, L_SUPPKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, C_CUSTKEY, L_PARTKEY
    L_LINESTATUS < O_ORDERSTATUS
    L_ORDERKEY < O_ORDERKEY
    L_PARTKEY < P_PARTKEY
    L_SUPPKEY < P_PARTKEY, S_SUPPKEY, C_CUSTKEY
    L_TAX < L_DISCOUNT
    N_NATIONKEY < C_NATIONKEY, S_NATIONKEY
    N_REGIONKEY < C_NATIONKEY, S_NATIONKEY, N_NATIONKEY, R_REGIONKEY
    O_CUSTKEY < P_PARTKEY, C_CUSTKEY
    O_SHIPPRIORITY < C_NATIONKEY, S_NATIONKEY, N_REGIONKEY, N_NATIONKEY, R_REGIONKEY
    P_SIZE < L_SUPPKEY, S_SUPPKEY, P_PARTKEY, C_CUSTKEY, L_PARTKEY
    R_REGIONKEY < C_NATIONKEY, S_NATIONKEY, N_REGIONKEY, N_NATIONKEY
    S_NATIONKEY < C_NATIONKEY, N_NATIONKEY
    S_SUPPKEY < L_SUPPKEY, P_PARTKEY, C_CUSTKEY
     */
  }
}

