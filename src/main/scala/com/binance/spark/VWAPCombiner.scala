package com.binance.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by ragrawal on 9/23/15.
  * Computes Mean
  */

//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
//https://ragrawal.wordpress.com/2015/11/03/spark-custom-udaf-example/
//https://www.jowanza.com/blog/a-gentle-intro-to-udafs-in-apache-spark
class VWAPCombiner() extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("vwap", DoubleType)))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("final", DoubleType)
  ))

  // Returned Data Type .
  def dataType: DataType = DoubleType

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.toDouble // set sum to one
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row): Double = {
    buffer.getDouble(0)
  }

}