package com.eighthours.sample.spark.domain.utils

import com.eighthours.sample.spark.domain.utils.json.UriJsonAdapter
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory

val moshi = Moshi.Builder()
        .add(KotlinJsonAdapterFactory())
        .add(UriJsonAdapter.Factory)
        .build()

inline fun <reified T> toJson(value: T): String {
    return moshi.adapter(T::class.java).toJson(value)
}

inline fun <reified T> fromJson(json: String): T? {
    return moshi.adapter(T::class.java).fromJson(json)
}
