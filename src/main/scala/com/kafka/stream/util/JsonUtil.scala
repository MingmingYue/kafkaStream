package com.kafka.stream.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Created by zhiweizhao on 2017/10/31.
  */
object JsonUtil {

  def createScalaObjectMapper :ObjectMapper = {
    val objectMapper = new ObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper
  }

  def getStringFromJson(node: JsonNode, field: String): String = {
    val fieldNode = node.get(field)
    if (fieldNode == null || StringUtil.isNullOrEmpty(fieldNode.asText())){
      return null
    }
    fieldNode.asText
  }

  def getLongFromJson(node: JsonNode, field: String): (Boolean, Long) = {
    val fieldNode = node.get(field)
    if (fieldNode == null || !fieldNode.canConvertToLong){
      return (false, -1)
    }
    (true, fieldNode.asLong)
  }

  def getIntFromJson(node: JsonNode, field: String): (Boolean, Int) = {
    val fieldNode = node.get(field)
    if (fieldNode == null){
      return (false, -1)
    }
    val value = fieldNode.asInt(-1)
    if (value == -1) {
      (false, -1)
    } else {
      (true, value)
    }
  }

}
