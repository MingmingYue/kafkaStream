package com.kafka.stream.deal

import java.util.Properties

import com.kafka.stream.model.Data
import com.kafka.stream.serializer.{JsonDeserializer, JsonSerializer}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer}
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * Created by zmm on 2018/11/2
  */
object JsonExampleTopology {

  private val broker = ConfigFactory.load.getString("kafka.broker")
  private val sourceTopic = ConfigFactory.load.getString("kafka.fill-request-log.topic")
  private val autoOffsetReset = ConfigFactory.load.getString("kafka.fill-request-log.auto-offset-reset")
  private val applicationId = ConfigFactory.load.getString("kafka.fill-request-log.application-id")
  private val streamThread = ConfigFactory.load.getInt("kafka.fill-request-log.stream-thread")

  def startStream(): KafkaStreams = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer.getClass)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Int.box(1))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Int.box(streamThread))

    val parseRequestLogDataProcessorSupplier = new ProcessorSupplier[String, String] {
      override def get(): Processor[String, String] = new JsonExample
    }

    val dataJsonDeserializer = new JsonDeserializer[Data](classOf[Data])
    val dataJsonSerializer = new JsonSerializer[Data]
    val serdeData = Serdes.serdeFrom(dataJsonSerializer, dataJsonDeserializer)

    val countStoreSupplier = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("data-statistics"),
      Serdes.String(), serdeData).withLoggingDisabled()

    val builder = new Topology

    builder.addSource("SOURCE", new StringDeserializer, new StringDeserializer, sourceTopic)
      .addProcessor("ParseRequestLogDataProcessor", parseRequestLogDataProcessorSupplier, "SOURCE")
      .addStateStore(countStoreSupplier, "ParseRequestLogDataProcessor")

    val stream = new KafkaStreams(builder, props)
    stream.start()
    stream
  }
}
