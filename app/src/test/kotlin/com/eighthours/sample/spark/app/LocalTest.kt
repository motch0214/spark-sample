package com.eighthours.sample.spark.app

import com.eighthours.sample.spark.app.utils.ProtoParquet
import com.eighthours.sample.spark.domain.calculation.EntryProtos
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.random.Random

class LocalTest {

    private val entrySize = 3

    private val file = "work/input/entries.parquet"

    @Test
    fun test() {
        val uri = Paths.get(file).toUri()

        Files.deleteIfExists(Paths.get(file))
        ProtoParquet.openWriter(uri, EntryProtos.Entry::class) { writer ->
            for (i in 0..entrySize) {
                val entry = EntryProtos.Entry.newBuilder()
                        .setId(i.toLong() + 1)
                        .setNumber(EntryProtos.NumberEntry.newBuilder().setTarget(Random.Default.nextDouble()))
                        .build()
                writer.write(entry)
            }
        }
    }
}
