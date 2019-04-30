package com.eighthours.sample.spark.app.utils

import com.google.protobuf.Message
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.proto.ProtoWriteSupport
import java.net.URI
import kotlin.reflect.KClass

internal object ProtoParquet {

    fun <M : Message> openWriter(uri: URI, messageType: KClass<M>, block: (ParquetWriter<M>) -> Unit) {
        WriterBuilder(uri, messageType).build().use(block)
    }
}

private class WriterBuilder<M : Message>(uri: URI, val messageType: KClass<M>)
    : ParquetWriter.Builder<M, WriterBuilder<M>>(org.apache.hadoop.fs.Path(uri)) {

    override fun self(): WriterBuilder<M> = this

    override fun getWriteSupport(conf: Configuration?): WriteSupport<M> {
        return ProtoWriteSupport(messageType.java)
    }
}
