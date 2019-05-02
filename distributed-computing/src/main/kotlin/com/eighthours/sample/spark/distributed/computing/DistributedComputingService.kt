package com.eighthours.sample.spark.distributed.computing

import java.net.URI

typealias ComputingResourceKey = String

typealias ComputingIdentifier = String

interface DistributedComputingService {

    fun submit(key: ComputingResourceKey, applicationJar: URI, args: List<String>): ComputingIdentifier
}
