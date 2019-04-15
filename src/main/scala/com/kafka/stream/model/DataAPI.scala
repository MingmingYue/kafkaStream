package com.kafka.stream.model

/**
  * Created by zmm on 2018/11/30
  */
case class Data(var id: Int = -1, var fillType: Int = -1, var country: String = null,  var request: Int = 0, var fullFill: Int = 0, var partFill: Int = 0, var checkError: Int = 0, var noFill: Int = 0) {}

object DataAPI {
  val REQUEST = "request"
  val FULL_FILL = 1
  val PART_FILL = 2
  val CHECK_ERROR = 3
  val NO_FILL = 4
}