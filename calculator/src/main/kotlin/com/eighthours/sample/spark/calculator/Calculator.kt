package com.eighthours.sample.spark.calculator

import com.eighthours.sample.spark.domain.calculation.*
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.NUMBER
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.STRING
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

class Calculator(private val spark: SparkSession) {

    fun calculate(parameters: CalculationParameters) {
        val input = spark.read().format("avro").load(*parameters.inputFiles.map { it.toString() }.toTypedArray())

        input.show()
        input.printSchema()

        val output = input.flatMap(Calculation(parameters.amplificationSize), Encoders.BINARY())
                .withColumnRenamed("value", RESULT_FIELD)

        output.show()
        output.printSchema()

        output.write().format("avro").save(parameters.outputDir.toString())
    }
}

typealias ResultBytes = ByteArray

class Calculation(private val amplificationSize: Int) : FlatMapFunction<Row, ResultBytes> {

    override fun call(row: Row): Iterator<ResultBytes> {
        val entry = EntryProtos.Entry.parseFrom(row.getAs<ByteArray>(ENTRY_FIELD))
        val results = calculate(entry)
        return results.map { it.toByteArray() }.iterator()
    }

    private fun calculate(entry: EntryProtos.Entry): List<ResultProtos.Result> {
        val value = when (entry.contentCase) {
            STRING -> "${entry.string.target}!!!"
            NUMBER -> "${entry.number.target}???"
            else -> throw IllegalArgumentException()
        }

        return (1..amplificationSize).map { i ->
            ResultProtos.Result.newBuilder()
                    .setId(entry.id)
                    .setValue("$value [$i]")
                    .build()
        }
    }
}
