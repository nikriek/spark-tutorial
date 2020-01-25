package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_set, lit}
import org.apache.spark.sql.types.ArrayType
import scala.collection.mutable

object Sindy {
  val columnName = "col"

  type AttributeSet = Seq[String]
  type Attribute = String

  case class Inclusion(subset: Attribute,
                       supersets: AttributeSet)

  case class InclusionList(subset: Attribute,
                           inclusionSupersets: Seq[AttributeSet])

  case class Value(value: String,
                   attribute: Attribute)

  /**
   * @param inputs List of file paths to CSV files
   */
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Create Dataframe from CSV files, one for each table.
    // List[String filepath] => List[DataFrame table]
    val tables = inputs.map(input => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(input)
    })

    // (1) Create cells from table data - DF with distinct (value, columnName)-tuples

    tables.map(tableDF => { // for each table
      tableDF.columns.map(columnName => { // for each column

        tableDF.select(tableDF(columnName)) // new DF with values from given column
          .distinct() // filter duplicate values
          .withColumnRenamed(columnName, "column_value")
          .withColumn("column_origin", lit(columnName)) // add "columnName"-column to DF

      }).foreach(df => df.show())
    })

//        .reduce((tuplesColumnA, tuplesColumnB) => tuplesColumnA.union(tuplesColumnB)) // Merge DFs (from columns)
//
//    }).reduce((tuplesTableA, tuplesTableB) => tuplesTableA.union(tuplesTableB)) // Merge DFs (from tables)
//      .as[Value]
//
//    // TODO: Look at partitioning
//
//    // (2) Create attribute sets from  (value, columnName)-tuples
//
//    // Attribute sets are grouping together attributes that have the same value in the column
//    val groupedAttributes = cells
//      .groupBy(cells.columns(0))
//      .agg(collect_set(columnName))
//    val attributeSets = groupedAttributes.select(groupedAttributes.columns(1)).as[Inclusion]
//
//    // Build inclusion list from attribute sets
//    val inclusionLists = attributeSets.flatMap(inclusion => {
//        // Generate set e.g [a,b] => [a, [b]], [b, [a]]
//        inclusion.supersets.map(attribute => (attribute, inclusion.supersets.filter(_ != attribute)))
//      }).as[Inclusion]
//
//    // Build result list by collecting attribute sets and
//    // merging them using the intersection
//    val results = inclusionLists
//      .groupBy(inclusionLists.columns(0))
//      .agg(collect_set(inclusionLists.columns(1)))
//      .as[InclusionList]
//      .filter(inclusionList => inclusionList.inclusionSupersets.exists(_.isEmpty))
//      .map(inclusionList => (inclusionList.subset, inclusionList.inclusionSupersets.flatten.distinct))
//      .as[Inclusion]
//
//    // Print results in desired format
//    results.foreach(result => println(s"${result.subset} < ${result.supersets.mkString(",")}"))

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

