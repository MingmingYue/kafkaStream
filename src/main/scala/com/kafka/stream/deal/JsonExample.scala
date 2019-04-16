package com.kafka.stream.deal

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import com.kafka.stream.model.{Data, DataAPI}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.KeyValueStore
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer

/**
  * Created by zmm on 2018/11/2
  */
class JsonExample extends Processor[String, String] {

  val log: Logger = Logger[JsonExample]

  private val defaultLength = 1000
  private var context: ProcessorContext = null
  private var dataStore: KeyValueStore[String, Data] = null
  private val punctuateInterval = ConfigFactory.load.getInt("kafka.fill-request-log.punctuate-interval")

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    import java.time.Duration
    context.schedule(Duration.ofMillis(punctuateInterval * 1000), PunctuationType.WALL_CLOCK_TIME, new Punctuator {
      override def punctuate(timestamp: Long): Unit = {
        log.info("[REQUEST-FILL]: start punctuate time {}", timestamp)
        synchronized {
          saveAllStatistics(dataStore)
        }
        context.commit()
        log.info("[REQUEST-FILL]: end punctuate time {}", System.currentTimeMillis)
      }
    })
    dataStore = context.getStateStore("data-statistics").asInstanceOf[KeyValueStore[String, Data]]
  }

  override def process(key: String, value: String): Unit = {
    if (value.nonEmpty) {
      try {
        val data: Data = new Data
        val root = Json.parse(value)
        val counterId = (root \ "counterid").as[String]
        if (DataAPI.REQUEST.equals(counterId)) {
          data.fillType = (root \ "ext" \ "fill").as[Int]
          data.country = (root \ "country").as[String]
        }
        dealData(data)
      } catch {
        case e: Exception => {
          log.error("[REQUEST-FILL]: exception value:{} error:{}", value, e.getMessage, e)
        }
      }
    }
  }

  def dealData(data: Data): Unit = {
    val key = getOfferStatisticsDataKey(data)
    val offerRequestStatisticsDataTemp = dataStore.get(key)
    if (offerRequestStatisticsDataTemp != null) {
      putValueToKey(key, offerRequestStatisticsDataTemp, data)
    } else {
      putValueToKey(key, data, data)
    }
  }

  private def putValueToKey(key: String, allData: Data, requestData: Data): Unit = {
    requestData.fillType match {
      case DataAPI.FULL_FILL =>
        allData.fullFill = allData.fullFill + 1
      case DataAPI.PART_FILL =>
        allData.partFill = allData.partFill + 1
      case DataAPI.CHECK_ERROR =>
        allData.checkError = allData.checkError + 1
      case DataAPI.NO_FILL =>
        allData.noFill = allData.noFill + 1
      case _ =>
        log.error(s"[REQUEST-FILL] counter: ${requestData.toString}")
    }
    dataStore.put(key, allData)
  }

  override def close(): Unit = {
    dataStore.close()
  }

  private def getOfferStatisticsDataKey(data: Data): String = {
    s"${data.country}"
  }

  private def insertDatabase(listBuffer: ListBuffer[Data]): Boolean = {
    listBuffer.foreach(ss => println(ss))
    true
  }

  private def saveAllStatistics(statistics: KeyValueStore[String, _]): Unit = {
    val iterator = statistics.all
    val keySet: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
    val dataList: scala.collection.mutable.ListBuffer[Data] = scala.collection.mutable.ListBuffer[Data]()
    try {
      while (iterator.hasNext) {
        val it: KeyValue[String, _] = iterator.next
        val value: Any = it.value
        value match {
          case data: Data => {
            data.request = data.fullFill + data.partFill + data.checkError + data.noFill
            dataList.+=(data)
            keySet.+=(it.key)
          }
          case _ => false
        }
      }
      if (dataList.nonEmpty) {
        var startValue = 0
        var insertCount = if (dataList.length % defaultLength == 0) dataList.length / defaultLength else dataList.length / defaultLength + 1
        while (insertCount > 0) {
          val dataListPart = dataList.slice(startValue, startValue + defaultLength)
          insertDatabase(dataListPart)
          startValue += defaultLength
          insertCount -= 1
        }
      }
      keySet.foreach(ss => statistics.delete(ss))
    } catch {
      case e: Exception => log.error("[REQUEST-FILL]: Unexpected error when forward statistics", e)
    }
    finally {
      iterator.close()
    }
  }
}

