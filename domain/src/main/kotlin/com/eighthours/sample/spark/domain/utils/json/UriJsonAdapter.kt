package com.eighthours.sample.spark.domain.utils.json

import com.squareup.moshi.JsonAdapter
import com.squareup.moshi.JsonAdapter.Factory
import com.squareup.moshi.JsonReader
import com.squareup.moshi.JsonWriter
import java.net.URI

class UriJsonAdapter : JsonAdapter<URI>() {

    companion object {

        val Factory = Factory { type, _, _ ->
            if (type == URI::class.java) {
                UriJsonAdapter().nullSafe()
            } else {
                null
            }
        }
    }

    override fun fromJson(reader: JsonReader): URI? {
        return URI.create(reader.nextString())
    }

    override fun toJson(writer: JsonWriter, value: URI?) {
        writer.value(value!!.toString())
    }
}
