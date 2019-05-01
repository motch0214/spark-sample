package com.eighthours.sample.spark.domain.calculation

fun EntryProtos.Entry.wrapper(): EntryWrapperProtos.EntryWrapper {
    return EntryWrapperProtos.EntryWrapper.newBuilder()
            .setId(this.id)
            .setBody(this.toByteString())
            .build()
}

fun EntryWrapperProtos.EntryWrapper.unwrap(): EntryProtos.Entry {
    return EntryProtos.Entry.parseFrom(this.body)
}
