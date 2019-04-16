package com.kafka.stream

import com.kafka.stream.deal.JsonExampleTopology

/**
  * Created by zmm on 2018/11/1
  */
object RealTimeApplication {
  def main(args: Array[String]): Unit = {
    JsonExampleTopology.startStream()
  }
}
