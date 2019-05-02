package com.eighthours.sample.spark.domain.calculation

import com.eighthours.sample.spark.domain.calculation.wrapper.ResultWrapperProtos

const val RESULT_FIELD = "result"

fun ResultWrapperProtos.Result.unwrap(): ResultProtos.Result {
    return ResultProtos.Result.parseFrom(this.result)
}
