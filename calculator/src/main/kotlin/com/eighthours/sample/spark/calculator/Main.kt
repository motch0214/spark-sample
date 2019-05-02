package com.eighthours.sample.spark.calculator

import com.eighthours.sample.spark.domain.utils.fromJson
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.*

private class Main

private val log = LoggerFactory.getLogger(Main::class.java)


fun main(args: Array<String>) {
    log.info("Run application: args=${Arrays.toString(args)}")

    if (args.isEmpty()) {
        throw IllegalArgumentException("usage: CalculationParameters")
    }

    // Load configurations from system properties
    val config = SparkConf()
    log.info("Spark configuration\n\t${config.toDebugString().replace("\n", "\n\t")}")

    val spark = SparkSession.builder().config(config).getOrCreate()
    Calculator(spark).calculate(fromJson(args[0])!!)
}
