package org.example
package com.bank.data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Calculation {

  System.setProperty("hadoop.home.dir", "D:\\OneDrive\\BigData\\Config\\Hadoop\\bin")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("BankData")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", false)
      .getOrCreate()

    val accountSchema = StructType(Seq(
      StructField("AccountTimeStamp", TimestampType, true),
      StructField("AccountID", LongType, true),
      StructField("AccountType", IntegerType, true)
    ))

    val loanSchema = StructType(Seq(
      StructField("LoanTimeStamp", TimestampType, true),
      StructField("LoanID", LongType, true),
      StructField("AccountID", IntegerType, true),
      StructField("LoanAmount", DecimalType.apply(3,3), true)
    ))
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(60))

    val account_file_path = "src/main/input/AccountInputData.Json"
    val loan_file_path = "src/main/input/LoanInputData.Json"

    val accountDf = spark.read.option("multiLine", false).schema(accountSchema).json(account_file_path)
    val loanDf = spark.read.option("multiLine", false).schema(loanSchema).json(loan_file_path)
    /*accountDf.printSchema()
    accountDf.show()
    loanDf.printSchema()
    loanDf.show()*/

    accountDf.createOrReplaceTempView("AccountData")
    loanDf.createOrReplaceTempView("LoanData")

    val totalLoansDF = spark.sql("select count(LoanID) From LoanData")

    totalLoansDF.show();

    spark.stop()
  }
}