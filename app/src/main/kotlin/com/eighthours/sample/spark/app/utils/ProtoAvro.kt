package com.eighthours.sample.spark.app.utils

import com.google.protobuf.Message
import org.apache.avro.file.*
import org.apache.avro.protobuf.ProtobufData
import org.apache.avro.protobuf.ProtobufDatumReader
import org.apache.avro.protobuf.ProtobufDatumWriter
import java.io.Closeable
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import kotlin.reflect.KClass

object ProtoAvro {

    class Writer<M : Message>(output: OutputStream, messageType: KClass<M>) : Closeable {

        constructor(file: Path, messageType: KClass<M>) : this(Files.newOutputStream(file).buffered(), messageType)

        private val writer = DataFileWriter(ProtobufDatumWriter(messageType.java)).also {
            it.create(ProtobufData.get().getSchema(messageType.java), output)
        }

        fun write(message: M) {
            writer.append(message)
        }

        override fun close() {
            writer.close()
        }
    }

    class Reader<M : Message> private constructor(input: SeekableInput, messageType: KClass<M>) : Closeable {

        constructor(file: Path, messageType: KClass<M>) : this(SeekableFileInput(file.toFile()), messageType)

        constructor(bytes: ByteArray, messageType: KClass<M>) : this(SeekableByteArrayInput(bytes), messageType)

        private val reader = DataFileReader(input, ProtobufDatumReader(messageType.java))

        fun read(): M? {
            if (!reader.hasNext()) {
                return null
            }
            return reader.next()
        }

        override fun close() {
            reader.close()
        }
    }
}
