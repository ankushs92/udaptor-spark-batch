package io

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.resolver.{FieldResolver, Resolver}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


class DataFile (fileContent: String, session: SparkSession) extends java.io.Serializable {
  import session.implicits._
  private val dataFrame: DataFrame = session.sqlContext.read.json(Seq(fileContent).toDS())
  private val intermediateDfs = new ListBuffer[DataFrame]()
  private val valuesColumnName = "values"
  private val pathColumnName = "outPath"

  import session.implicits._

  def splitFormat(format: String): (String, Array[String]) = {
    val split = format.split("öø")
    val path = split(1).split("ø")

    return (split(0), path)
  }

  def registerMap(inputFormat: String, outputFormat: String): Unit = {
    val (inFileName: String, inPath: Array[String]) = splitFormat(inputFormat)
    val strings = outputFormat.split("ö")
    val outFileName = strings(0)
    val outPath = strings(1)

    var selection = dataFrame

    inPath.slice(0, inPath.length - 1).foreach{p =>
      val field = new FieldResolver(p)
      val name = field.name

      if (field.hasArrayChild){
        selection = selection.select(functions.explode(selection(name)))
          .withColumnRenamed("col", valuesColumnName)
          .toDF()
      }

      if (field.hasObjectChild) {
        selection.columns
        val columnName = if (selection.columns.contains(name)) name else "values"

        selection = selection.select(Symbol(columnName + ".*"))
          .withColumn(pathColumnName, functions.lit(outFileName + "ö" + outPath))
//          .withColumnRenamed(columnName, valuesColumnName)
//          .groupBy(pathColumnName)
//          .agg(functions.collect_list(valuesColumnName).as(valuesColumnName))
          .toDF()
      }

      if (field.hasValueChild) {
        val symbol = if (selection.columns.contains(p)) Symbol(p) else 'values
        selection = selection.select(symbol)
          .withColumn(pathColumnName, functions.lit(outFileName + "ö" + outPath))
          .withColumnRenamed(p, valuesColumnName)
          .toDF()
      }
    }

    val p = inPath.last
    val field = new FieldResolver(p)
    val name = field.name

    if (field.hasArrayChild) {
      selection = selection.select(functions.explode(selection(name))).toDF()
    }

//    if (field.hasObjectChild) {
//      val symbol = if (selection.columns.contains(name)) Symbol(name) else 'col
//
//      val frame = selection.select(functions.when(symbol.getItem(name).isNull, "null").otherwise('col.getItem(name)) as name)
//        .withColumn(pathColumnName, functions.lit(outFileName + "ö" + outPath))
//        .withColumnRenamed(name, valuesColumnName)
//        .groupBy(pathColumnName)
//        .agg(functions.collect_list(valuesColumnName).as(valuesColumnName))
//        .toDF()
//      intermediateDfs += frame
//    }

    if (field.hasValueChild) {
      val columnName = if(selection.columns.contains(name)) name else valuesColumnName
      val symbol = Symbol(columnName)

      val frame = selection.select(functions.when(symbol.isNull, "null").otherwise(symbol) as columnName)
        .withColumn(pathColumnName, functions.lit(outFileName + "ö" + outPath))
        .withColumnRenamed(columnName, valuesColumnName)
        .toDF()
      intermediateDfs += frame
    }

  }

  def performMapsAndReturnFile(): File = {


    val reduced: DataFrame = intermediateDfs.reduce { (df1, df2) =>
      df1.union(df2)
    }.toDF()


    val column = reduced(pathColumnName)
    val splitColumnName = "_split"

    val withTempColumn = reduced.withColumn(splitColumnName, functions.split(column, "öø"))

    val splitDF = withTempColumn.select(
        withTempColumn(splitColumnName).getItem(0).as("fileName"),
        withTempColumn(splitColumnName).getItem(1).as("filePath"),
        withTempColumn(valuesColumnName).as(valuesColumnName)
    ).drop(splitColumnName)

    val frame = splitDF.withColumn("splitPath",
      functions.split(splitDF("filePath"), "ø"))
      .drop("filePath")

//
//    frame.show(false)
//
//
//    val folded = frame.javaRDD.mapToPair(row => {
//
//      val list = row.getList(2)
//      val str = row.getString(0)
//      val newList = new java.util.ArrayList[String]()
//      newList.add(str)
//      newList.addAll(list)
//      (newList, row)
//    }).mapValues((row: Row) => row.toSeq.asInstanceOf[Seq[String]])
//      .foldByKey(Seq[String](), (acc: Seq[String], sequence: Seq[String]) => {
//        acc :+ sequence(1)
//    }).mapToPair((key, value) => {
//
//    })


      //.collect().forEach(println)





//      .reduceByKey((v1: Seq[String], v2: Seq[String]) => {
//        val val1 = v1(1)
//      if (value.isInstanceOf[List[String]]){
//        Row.fromSeq(value.asInstanceOf[List[String]] ::: List(v2.getString(1)))
//      } else {
//        Row.fromSeq(List(value, v2.getString(1)))
//      }
//    }).collect().forEach(println)



    val resolveJsonUdf = udf((row: Row) => resolveJson(row))

    val withJsonStrings = frame.withColumn("jsonObject", resolveJsonUdf(functions.struct(frame("splitPath"), frame(valuesColumnName))))
      .drop("splitPath")
      .drop(valuesColumnName)

    withJsonStrings.show(false)

    val outputJson = withJsonStrings.javaRDD.mapToPair(row => (row.getString(0), row.getString(1)))
      .reduceByKey((v1, v2) => {
        val node = new ObjectMapper().readTree(v1.asInstanceOf[String])
        val node2 = new ObjectMapper().readTree(v2.asInstanceOf[String])
        val str = new ObjectMapper().writeValueAsString(Resolver.merge(node, node2))
        str
      })

    val tuple = outputJson.take(1).get(0)

    val fileName = tuple._1
    val jsonString = tuple._2

    val file = java.io.File.createTempFile("tmp", fileName)

    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(jsonString)
    writer.close()
    println(file.getAbsolutePath)
    return file


//    withJsonStrings
  }

  def resolveJson(row: Row): String = {
    val path = row.getList[String](0)
    val value = row.get(1)
    if (!value.isInstanceOf[List[String]]) {
      val strings = new util.ArrayList[String]()
      strings.add(value.toString)
      return Resolver.resolveJson(path, strings)
    }

    return Resolver.resolveJson(path, row.getList(1))

  }

}
