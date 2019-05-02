package com.eighthours.sample.spark.app

import com.eighthours.sample.spark.app.utils.ProtoAvro
import com.eighthours.sample.spark.distributed.computing.emr.impl.EmrDistributedComputingService
import com.eighthours.sample.spark.domain.calculation.CalculationParameters
import com.eighthours.sample.spark.domain.calculation.EntryProtos
import com.eighthours.sample.spark.domain.calculation.wrapper
import com.eighthours.sample.spark.domain.calculation.wrapper.EntryWrapperProtos
import com.eighthours.sample.spark.domain.utils.toJson
import org.junit.Test
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.ByteArrayOutputStream
import java.net.URI

class EmrTest {

    private val bucket = "s3://bucket.s3-ap-northeast-1.amazonaws.com"

    private val inputFile = URI.create("$bucket/data/input/entries.avro")

    private val outputDir = URI.create("$bucket/data/output")

    private val entrySize = 3

    private val amplificationSize = 5;

    private val resourceKey = "default"

    private val application = URI.create("$bucket/app/calculator-0.1.0-SHAPSHOT-all.jar")

    @Test
    fun test() {
        val service = EmrDistributedComputingService()

        // Create input file
        val bytes = ByteArrayOutputStream()
        ProtoAvro.Writer(bytes, EntryWrapperProtos.Entry::class).use { writer ->
            for (i in 1..entrySize) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong())
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(100.0 + i))
                        .build()
                writer.write(entry.wrapper())
            }
        }

        val s3 = S3Client.create()
        s3.putObject(PutObjectRequest.builder()
                .bucket("bucket")
                .build(), RequestBody.fromBytes(bytes.toByteArray()))

        val parameters = CalculationParameters(
                amplificationSize = amplificationSize,
                inputFiles = listOf(inputFile),
                outputDir = outputDir)
        val id = service.submit(resourceKey, application, listOf(toJson(parameters)))
    }
}
