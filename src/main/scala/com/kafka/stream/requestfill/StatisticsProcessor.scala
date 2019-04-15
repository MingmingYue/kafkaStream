package com.kafka.stream.requestfill

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
class StatisticsProcessor extends Processor[String, String] {

  val log: Logger = Logger[StatisticsProcessor]

  private val defaultLength = 1000
  private var context: ProcessorContext = null
  private var offerRequestStatisticsDataStore: KeyValueStore[String, Data] = null
  private val punctuateInterval = ConfigFactory.load.getInt("kafka.fill-request-log.punctuate-interval")

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    import java.time.Duration
    context.schedule(Duration.ofMillis(punctuateInterval * 1000), PunctuationType.WALL_CLOCK_TIME, new Punctuator {
      override def punctuate(timestamp: Long): Unit = {
        log.info("[OFFER-REQUEST-FILL]: start punctuate time {}", timestamp)
        synchronized {
          saveAllStatistics(offerRequestStatisticsDataStore)
        }
        context.commit()
        log.info("[OFFER-REQUEST-FILL]: end punctuate time {}", System.currentTimeMillis)
      }
    })
    offerRequestStatisticsDataStore = context.getStateStore("offerRequestStatisticsData-statistics").asInstanceOf[KeyValueStore[String, Data]]
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
    val offerRequestStatisticsDataTemp = offerRequestStatisticsDataStore.get(key)
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
        log.error(s"[OFFER-REQUEST-FILL] counter: ${requestData.toString}")
    }
    offerRequestStatisticsDataStore.put(key, allData)
  }

  override def close(): Unit = {
    offerRequestStatisticsDataStore.close()
  }

  private def getOfferStatisticsDataKey(offerRequestStatisticsData: Data): String = {
    s"${offerRequestStatisticsData.country}"
  }

  private def insertDatabase(listBuffer: ListBuffer[Data]): Boolean = {
    listBuffer.foreach(ss => println(ss))
    true
  }

  private def saveAllStatistics(statistics: KeyValueStore[String, _]): Unit = {
    val iterator = statistics.all
    val keySet: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
    val offerRequestStatisticsDataList: scala.collection.mutable.ListBuffer[Data] = scala.collection.mutable.ListBuffer[Data]()
    try {
      while (iterator.hasNext) {
        val it: KeyValue[String, _] = iterator.next
        val value: Any = it.value
        value match {
          case data: Data => {
            data.request = data.fullFill + data.partFill + data.checkError + data.noFill
            offerRequestStatisticsDataList.+=(data)
            keySet.+=(it.key)
          }
          case _ => false
        }
      }
      if (offerRequestStatisticsDataList.length > 0) {
        var startValue = 0
        var insertCount = if (offerRequestStatisticsDataList.length % defaultLength == 0) offerRequestStatisticsDataList.length / defaultLength else offerRequestStatisticsDataList.length / defaultLength + 1
        while (insertCount > 0) {
          val offerRequestStatisticsDataListPart = offerRequestStatisticsDataList.slice(startValue, startValue + defaultLength)
          insertDatabase(offerRequestStatisticsDataListPart)
          startValue += defaultLength
          insertCount -= 1
        }
      }
      keySet.foreach(ss => statistics.delete(ss))
    } catch {
      case e: Exception => log.error("[OFFER-REQUEST-FILL]: Unexpected error when forward statistics", e)
    }
    finally {
      iterator.close()
    }
  }
}

