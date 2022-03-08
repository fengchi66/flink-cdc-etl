package com.echo.data

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.concurrent.TimeUnit

object Test2 {

  def main(args: Array[String]): Unit = {
    // env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(3000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(20, TimeUnit.SECONDS)))

    // tableEnv
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    tableEnv.executeSql(
      """
        |CREATE TABLE test (
        |  id BIGINT,
        |  ts TIMESTAMP_LTZ(3),
        |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'cdc2.inventory.test	',
        | 'properties.bootstrap.servers' = 'ecs01:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'debezium-json',
        | 'scan.startup.mode' = 'earliest-offset',
        | 'debezium-json.timestamp-format.standard' = 'ISO-8601'
        |)
        |""".stripMargin)

    tableEnv.executeSql("SELECT * FROM test").print()


  }

}
