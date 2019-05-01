package com.eighthours.sample.spark.domain.calculation


fun ResultWrapperProtos.ResultWrapper.unwrap(): ResultProtos.Result {
    return ResultProtos.Result.parseFrom(this.result)
}
