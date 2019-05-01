package com.eighthours.sample.spark.domain.calculation

import com.eighthours.sample.spark.domain.calculation.wrapper.ResultWrapperProtos

fun ResultWrapperProtos.Result.unwrap(): ResultProtos.Result {
    return ResultProtos.Result.parseFrom(this.result)
}
