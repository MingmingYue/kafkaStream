package com.kafka.stream.serializer

import java.nio.charset.Charset
import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.kafka.stream.util.JsonUtil
import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.serialization.Serializer

/**
  * Created by zhiweizhao on 2017/10/31.
  */
class JsonSerializer[T] extends Serializer[T] {

  val log = Logger[JsonSerializer[T]]
  val mapper = JsonUtil.createScalaObjectMapper

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = {
    try {
      return mapper.writeValueAsString(data).getBytes(Charset.forName("UTF-8"))
    } catch {
      case e: JsonProcessingException => {
        log.error("serialize error, topic {} value {} error {}", topic, data, e.getMessage, e)
        e.printStackTrace()
      }
    }
    return null
  }

  override def close(): Unit = {}
}
