package com.echo.data

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.concurrent.TimeUnit

object AvroApp {

  def main(args: Array[String]): Unit = {
    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(30000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(20, TimeUnit.SECONDS)))

    // tableEnv
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    tableEnv.executeSql(
      """
        |CREATE TABLE customers (
        |  id BIGINT,
        |  first_name STRING,
        |  last_name STRING,
        |  email STRING
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'cdc.inventory.customers',
        | 'properties.bootstrap.servers' = 'kafka_dev:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'debezium-avro-confluent',
        | 'scan.startup.mode' = 'earliest-offset',
        | 'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
        |)
        |""".stripMargin)

    tableEnv.executeSql("SELECT * FROM customers").print()
  }

}
