package com.eighthours.sample.spark.distributed.computing.emr.impl

import com.eighthours.sample.spark.distributed.computing.ComputingIdentifier
import com.eighthours.sample.spark.distributed.computing.ComputingResourceKey
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.emr.EmrAsyncClient
import software.amazon.awssdk.services.emr.model.*
import java.net.URI
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

typealias ClusterId = String

typealias StepId = String

class EmrDistributedComputingService {

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java.enclosingClass)
    }

    private val emr by lazy { EmrAsyncClient.create() }

    private val clusters: ConcurrentMap<ComputingResourceKey, () -> CompletableFuture<ClusterId>> = ConcurrentHashMap()

    private val statuses: ConcurrentMap<ComputingIdentifier, EmrComputingStatus> = ConcurrentHashMap()

    fun submit(key: ComputingResourceKey, applicationJar: URI, args: List<String>): ComputingIdentifier {
        val identifier = UUID.randomUUID().toString()
        log.info("Submit computing: id=$identifier, resource=$key, application=$applicationJar, args=$args")
        statuses[identifier] = EmrComputingStatus.Waiting

        val clusterIdProvider = clusters.computeIfAbsent(key) {
            { launchCluster(key) }
        }

        clusterIdProvider.invoke().thenApply { clusterId ->
            log.info("Submit step to cluster: id=$identifier, cluster=$clusterId")
            val config = loadConfig(key)

            // See https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html
            val step = HadoopJarStepConfig.builder()
                    .jar("command-runner.jar")
                    // See https://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit
                    .args("spark-submit",
                            "--deploy-mode", "cluster",
                            *config.getStringList("spark-submit-args").toTypedArray(),
                            applicationJar.toString(), *args.toTypedArray())
                    .build()
            val response = emr.addJobFlowSteps(AddJobFlowStepsRequest.builder()
                    .jobFlowId(clusterId)
                    .steps(StepConfig.builder()
                            .name(identifier)
                            .actionOnFailure(ActionOnFailure.CONTINUE)
                            .hadoopJarStep(step)
                            .build())
                    .build())

            response.thenApply {
                log.info("Step accepted: id=$identifier, cluster=$clusterId")
                // Assuming status is Waiting
                statuses[identifier] = EmrComputingStatus.Queued(it.stepIds()[0])
            }
        }

        return identifier
    }

    private fun launchCluster(key: ComputingResourceKey): CompletableFuture<ClusterId> {
        log.info("Launch cluster: key=$key")
        val config = loadConfig(key)

        // See https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html
        val response = emr.runJobFlow(RunJobFlowRequest.builder()
                .name(key)
                .applications(Application.builder()
                        .name("Spark")
                        .build())
                // Release Label (e.g. "emr-5.21.0")
                // See https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html
                .releaseLabel(config.getString("release-label"))
                // See https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-instances-guidelines.html
                .instances(JobFlowInstancesConfig.builder()
                        .instanceGroups(
                                // TODO ...
                                InstanceGroupConfig.builder()
                                        .instanceRole(InstanceRoleType.MASTER)
                                        .instanceCount(1)
                                        .instanceType("m4.large")
                                        .build(),
                                InstanceGroupConfig.builder()
                                        .instanceRole(InstanceRoleType.CORE)
                                        .instanceCount(1)
                                        .instanceType("c4.large")
                                        .build(),
                                InstanceGroupConfig.builder()
                                        .instanceRole(InstanceRoleType.TASK)
                                        .instanceCount(4)
                                        .instanceType("c4.large")
                                        .build())
                        .build())
                .build())

        return response.thenApply {
            log.info("Cluster launched: key=$key, cluster=${it.jobFlowId()}")
            it.jobFlowId()
        }
    }

    private fun loadConfig(key: ComputingResourceKey): Config {
        return ConfigFactory.load("distributed-computing")
                .getConfig("computing-resources.$key")
    }
}
