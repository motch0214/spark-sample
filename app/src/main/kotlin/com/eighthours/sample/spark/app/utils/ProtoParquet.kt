package com.eighthours.sample.spark.app.utils

import com.google.protobuf.Message
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.proto.ProtoParquetWriter
import java.net.URI
import kotlin.reflect.KClass

internal object ProtoParquet {

    fun <M : Message> openWriter(uri: URI, messageClass: KClass<M>, block: (ParquetWriter<M>) -> Unit) {
        ProtoParquetWriter<M>(Path(uri), messageClass.java).use(block)
    }
}
