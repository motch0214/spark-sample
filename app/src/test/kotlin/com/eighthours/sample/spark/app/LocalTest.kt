package com.eighthours.sample.spark.app

import com.eighthours.sample.spark.app.utils.ProtoAvro
import com.eighthours.sample.spark.domain.calculation.*
import com.eighthours.sample.spark.domain.utils.toJson
import org.junit.Before
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.random.Random

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

    @Test
    fun test() {
        // Create input file
        Files.createDirectories(inputFile.parent)
        ProtoAvro.Writer(inputFile, EntryWrapperProtos.EntryWrapper::class).use { writer ->
            for (i in 1..entrySize) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong())
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(Random.Default.nextDouble()))
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
            ProtoAvro.Reader(file, ResultWrapperProtos.ResultWrapper::class).use { reader ->
                while (true) {
                    val result = reader.read()?.unwrap() ?: break
                    results.add(result)
                }
            }
        }

        println(results)
    }
}
