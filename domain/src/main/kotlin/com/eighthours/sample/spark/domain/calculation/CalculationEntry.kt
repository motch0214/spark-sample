package com.eighthours.sample.spark.domain.calculation

import com.eighthours.sample.spark.domain.calculation.wrapper.EntryWrapperProtos

fun EntryProtos.Entry.wrapper(): EntryWrapperProtos.Entry {
    return EntryWrapperProtos.Entry.newBuilder()
            .setId(this.id)
            .setEntry(this.toByteString())
            .build()
}

fun EntryWrapperProtos.Entry.unwrap(): EntryProtos.Entry {
    return EntryProtos.Entry.parseFrom(this.entry)
}
