package com.eighthours.sample.spark.calculator

import com.eighthours.sample.spark.domain.calculation.CalculationParameters
import com.eighthours.sample.spark.domain.calculation.EntryProtos
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.NUMBER
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.STRING
import com.eighthours.sample.spark.domain.calculation.ResultProtos
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes

class Calculator(private val spark: SparkSession) {

    fun calculate(parameters: CalculationParameters) {
        val input = spark.read().format("avro").load(*parameters.inputFiles.map { it.toString() }.toTypedArray())

        input.show()
        input.printSchema()

        val output = input.withColumn("result", udf(Function, DataTypes.BinaryType).apply(Column("entry")))

        output.show()
        output.printSchema()

        output.write().format("avro").save(parameters.outputDir.toString())
    }
}

object Function : UDF1<ByteArray, ByteArray> {

    override fun call(bytes: ByteArray): ByteArray {
        val entry = EntryProtos.Entry.parseFrom(bytes)
        val result = calculate(entry)
        return result.toByteArray()
    }

    private fun calculate(entry: EntryProtos.Entry): ResultProtos.Result {
        val value = when (entry.contentCase) {
            STRING -> "${entry.string.target}!!!"
            NUMBER -> "${entry.number.target}???"
            else -> throw IllegalArgumentException()
        }

        return ResultProtos.Result.newBuilder()
                .setId(entry.id)
                .setValue(value)
                .build()
    }
}
