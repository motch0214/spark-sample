package com.eighthours.sample.spark.app

import com.eighthours.sample.spark.app.utils.ProtoAvro
import com.eighthours.sample.spark.domain.calculation.*
import com.eighthours.sample.spark.domain.calculation.wrapper.EntryWrapperProtos
import com.eighthours.sample.spark.domain.calculation.wrapper.ResultWrapperProtos
import com.eighthours.sample.spark.domain.utils.toJson
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Paths

class LocalTest {

    private val entrySize = 3

    private val amplificationSize = 5;

    private val inputFile = Paths.get("work/input/entries.avro")

    private val outputDir = Paths.get("work/output/results")

    @Before
    fun cleanup() {
        Files.walk(Paths.get("work")).sorted(Comparator.reverseOrder()).forEachOrdered {
            Files.delete(it)
        }
    }

    @Before
    fun setupProperties() {
        System.setProperty("spark.app.name", "spark-sample-test")
        System.setProperty("spark.master", "local")
    }

    @Test
    fun test() {
        // Create input file
        Files.createDirectories(inputFile.parent)
        ProtoAvro.Writer(inputFile, EntryWrapperProtos.Entry::class).use { writer ->
            for (i in 1..entrySize) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong())
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(100.0 + i))
                        .build()
                writer.write(entry.wrapper())
            }
        }

        // Call Spark locally
        val parameters = CalculationParameters(
                amplificationSize = amplificationSize,
                inputFiles = listOf(inputFile.toUri()),
                outputDir = outputDir.toUri())
        com.eighthours.sample.spark.calculator.main(arrayOf(toJson(parameters)))

        // Read output file
        val results = mutableListOf<ResultProtos.Result>()
        Files.list(outputDir).filter { it.toString().endsWith(".avro") }.forEach { file ->
            ProtoAvro.Reader(file, ResultWrapperProtos.Result::class).use { reader ->
                while (true) {
                    val result = reader.read()?.unwrap() ?: break
                    results.add(result)
                }
            }
        }

        // Assert results
        val expected = (1..entrySize).flatMap { id ->
            (1..amplificationSize).map { index ->
                ResultProtos.Result.newBuilder()
                        .setId(id.toLong())
                        .setValue("${(100.0 + id) * index}")
                        .build()
            }
        }
        assertThat(results).isEqualTo(expected)
    }
}
