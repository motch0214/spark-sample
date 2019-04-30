package com.eighthours.sample.spark.domain.calculation

data class CalculationParameters(
        val amplificationSize: Int,
        val inputFiles: List<String>,
        val outputDir: String
)
