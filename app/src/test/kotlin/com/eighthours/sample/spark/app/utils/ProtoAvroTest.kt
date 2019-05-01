package com.eighthours.sample.spark.app.utils

import com.eighthours.sample.spark.domain.calculation.EntryProtos
import com.eighthours.sample.spark.domain.calculation.EntryWrapperProtos
import com.eighthours.sample.spark.domain.calculation.unwrap
import com.eighthours.sample.spark.domain.calculation.wrapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.ByteArrayOutputStream
import kotlin.math.exp

class ProtoAvroTest {

    @Test
    fun test() {
        // Write
        val output = ByteArrayOutputStream()
        val expected = mutableListOf<EntryProtos.Entry>()
        ProtoAvro.Writer(output, EntryWrapperProtos.EntryWrapper::class).use { writer ->
            for (i in 1..5) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong())
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(exp(i.toDouble())))
                        .build()
                writer.write(entry.wrapper())
                expected.add(entry)
            }
        }
        val bytes = output.toByteArray()

        // Read
        val actual = mutableListOf<EntryProtos.Entry>()
        ProtoAvro.Reader(bytes, EntryWrapperProtos.EntryWrapper::class).use { reader ->
            while (true) {
                val message = reader.read() ?: break
                actual.add(message.unwrap())
            }
        }

        assertThat(actual).isEqualTo(expected)
    }
}
