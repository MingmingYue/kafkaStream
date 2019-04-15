package com.kafka.stream.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

/**
  * Created by zmm on 2018/11/2
  */
object DateTimeUtil {

  def getStringToTime(stringTime: String): Int = {
    val s = new SimpleDateFormat("yyyy-MM-dd'T'HH")
    (s.parse(stringTime).getTime / 1000).toInt
  }

  /**
    * 去当前时间往前的第一个逢五分钟
    */
  def getNearFiveMinute(time: Long): Int = {
    val now: Date = new Date(time)
    val calendar = Calendar.getInstance()
    calendar.setTime(now)
    val minute = calendar.get(Calendar.MINUTE)
    val overTime = minute - minute % 5
    calendar.set(Calendar.MINUTE, overTime)
    calendar.set(Calendar.SECOND, 0)
    (calendar.getTimeInMillis / 1000).toInt
  }

  // 获取x天后zone时区0点的时间 注意格式化日期类型必须是yyyy-MM-dd
  def timeToDateSecond(time: Long, zone: Int, xDay: Int): Int = {
    val format = "yyyy-MM-dd"
    val now: Date = new Date(time)
    val calendar = Calendar.getInstance()
    calendar.setTime(now)
    calendar.add(Calendar.DAY_OF_YEAR, xDay)
    val xDayTime = (calendar.getTimeInMillis / 1000).toInt
    val timeDate = timeToDate(xDayTime, format, zone)
    getTimeByZone(timeDate, format, zone)
  }

  def timeToDate(time: Long, format: String, zone: Int): String = {
    val sdf = new SimpleDateFormat(format)
    if (zone >= 0) {
      sdf.setTimeZone(TimeZone.getTimeZone("GMT+" + zone))
    } else {
      sdf.setTimeZone(TimeZone.getTimeZone("GMT" + zone))
    }
    sdf.format(time * 1000)
  }

  def getTimeByZone(str: String, format: String, zone: Int): Int = {
    val sdf = new SimpleDateFormat(format)
    val time = sdf.parse(str)
    (time.getTime / 1000).toInt
  }

  def getNowTime(): Int = {
    val time = new Date().getTime
    (time / 1000).toInt
  }
}
