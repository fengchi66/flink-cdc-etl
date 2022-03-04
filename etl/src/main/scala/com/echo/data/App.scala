package com.echo.data

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object App {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(30000)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

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
        | 'format' = 'debezium-json',
        | 'debezium-json.schema-include' = 'true'
        |)
        |""".stripMargin)

    tableEnv.executeSql("SELECT * FROM customers").print()
  }

}
