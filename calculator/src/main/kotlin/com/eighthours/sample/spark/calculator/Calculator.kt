package com.eighthours.sample.spark.calculator

import com.eighthours.sample.spark.domain.calculation.CalculationParameters
import com.eighthours.sample.spark.domain.calculation.EntryProtos
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.NUMBER
import com.eighthours.sample.spark.domain.calculation.EntryProtos.Entry.ContentCase.STRING
import com.eighthours.sample.spark.domain.calculation.ResultProtos
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import scala.Tuple2

class Calculator(private val spark: SparkSession) {

    fun calculate(parameters: CalculationParameters) {
        spark.read().format("avro").load(*parameters.inputFiles.toTypedArray())
                .map(Mapper, Encoders.tuple(Encoders.LONG(), Encoders.BINARY()))
                .write().format("avro").save(parameters.outputDir)
    }
}

object Mapper : MapFunction<Row, Tuple2<Long, ByteArray>> {

    override fun call(row: Row): Tuple2<Long, ByteArray> {
        val bytes: ByteArray = row.getAs("body")
        val entry = EntryProtos.Entry.parseFrom(bytes)
        val result = calculate(entry)
        return Tuple2(result.id, result.toByteArray())
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
