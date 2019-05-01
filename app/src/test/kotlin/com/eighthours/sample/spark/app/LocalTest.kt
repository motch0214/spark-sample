package com.eighthours.sample.spark.app

import com.eighthours.sample.spark.app.utils.ProtoParquet
import com.eighthours.sample.spark.domain.calculation.CalculationParameters
import com.eighthours.sample.spark.domain.calculation.EntryProtos
import com.eighthours.sample.spark.domain.calculation.EntryWrapperProtos
import com.eighthours.sample.spark.domain.calculation.wrapper
import com.eighthours.sample.spark.domain.utils.toJson
import org.apache.commons.io.FileUtils
import org.junit.Before
import org.junit.Test
import java.nio.file.Paths
import kotlin.random.Random

class LocalTest {

    private val entrySize = 3

    private val amplificationSize = 5;

    private val inputFile = "work/input/entries.parquet"

    private val outputDir = "work/output/results"

    @Before
    fun cleanup() {
        FileUtils.deleteDirectory(Paths.get("work").toFile())
    }

    @Test
    fun test() {
        val uri = Paths.get(inputFile).toUri()

        ProtoParquet.openWriter(uri, EntryWrapperProtos.EntryWrapper::class) { writer ->
            for (i in 0..entrySize) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong() + 1)
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(Random.Default.nextDouble()))
                        .build()
                writer.write(entry.wrapper())
            }
        }

        val parameters = CalculationParameters(
                amplificationSize = amplificationSize,
                inputFiles = listOf(uri.toString()),
                outputDir = Paths.get(outputDir).toUri().toString())
        com.eighthours.sample.spark.calculator.main(arrayOf(toJson(parameters)))
    }
}
