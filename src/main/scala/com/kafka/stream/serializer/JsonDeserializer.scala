package com.kafka.stream.serializer

import java.io.IOException
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.serialization.Deserializer

/**
  * Created by zhiweizhao on 2017/10/31.
  */
class JsonDeserializer[T] extends Deserializer[T] {

  val log = Logger[JsonDeserializer[T]]
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  var deserializerClass: Class[T] = null

  def this(deClass: Class[T]) = {
    this
    deserializerClass = deClass
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    if (deserializerClass == null) {
      deserializerClass = configs.get("serializedClass").asInstanceOf[Class[T]]
    }
  }

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    if (data == null) {
      return null.asInstanceOf[T]
    }
    try {
      return mapper.readValue(data, deserializerClass)
    }
    catch {
      case e: IOException =>
        log.error("deserialize error, topic {} value {} error {}", topic, new String(data), e.getMessage, e)
    }
    null.asInstanceOf[T]
  }
}
