package com.echo.data

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object StreamApp {

  val TOPIC = "cdc2.inventory.test"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(30000)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ecs01:9092")
    properties.setProperty("group.id", "testGroup")

    val kafkaConsumer = new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromEarliest()

    env.addSource(kafkaConsumer).print()

    env.execute("job")

  }

}
