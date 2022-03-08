package com.echo.data

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.concurrent.TimeUnit

object JsonApp {
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
        |CREATE TABLE orders (
        |  id BIGINT,
        |  type BIGINT,
        |  status BIGINT,
        |  buyer_id BIGINT,
        |  seller_id BIGINT,
        |  price DECIMAL(20),
        |  deposit_price DECIMAL(20),
        |  final_price DECIMAL(20),
        |  express_price DECIMAL(20),
        |  coupon_price DECIMAL(20),
        |  paid_at TIMESTAMP(3),
        |  express_id BIGINT,
        |  express_created_at TIMESTAMP(3),
        |  finished_at TIMESTAMP(3),
        |  created_at TIMESTAMP(3),
        |  updated_at TIMESTAMP(3),
        |  transaction_id BIGINT,
        |  package_id BIGINT,
        |  expire_at TIMESTAMP(3),
        |  sub_mch_id STRING,
        |  related_id BIGINT,
        |  recharge_stage BIGINT
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'cdc.orders.orders',
        | 'properties.bootstrap.servers' = 'kafka_dev:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'debezium-json',
        | 'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tableEnv.executeSql("SELECT * FROM orders").print()
  }

}
