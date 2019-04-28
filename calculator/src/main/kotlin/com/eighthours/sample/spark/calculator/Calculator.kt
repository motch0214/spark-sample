package com.eighthours.sample.spark.calculator

import com.eighthours.sample.spark.domain.calculation.CalculationParameters
import org.apache.spark.sql.SparkSession

class Calculator(private val spark: SparkSession) {

    fun calculate(parameters: CalculationParameters) {
        val input = spark.read().parquet(*parameters.inputFiles.toTypedArray())

        // TODO
        input.show()
        input.printSchema()
    }
}
