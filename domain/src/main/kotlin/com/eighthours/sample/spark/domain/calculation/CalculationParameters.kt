package com.eighthours.sample.spark.domain.calculation

import java.net.URI

data class CalculationParameters(
        val amplificationSize: Int,
        val inputFiles: List<URI>,
        val outputDir: URI
)
