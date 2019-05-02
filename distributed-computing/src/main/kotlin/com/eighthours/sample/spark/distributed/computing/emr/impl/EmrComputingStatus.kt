package com.eighthours.sample.spark.distributed.computing.emr.impl

sealed class EmrComputingStatus {

    object Waiting : EmrComputingStatus()

    class Queued(val stepId: StepId) : EmrComputingStatus()
}
