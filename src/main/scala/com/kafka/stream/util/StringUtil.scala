package com.kafka.stream.util

import java.security.MessageDigest

/**
  * Created by zhiweizhao on 2017/11/6.
  */
object StringUtil {

  def isNullOrEmpty(string: String): Boolean = {
    if (string == null || string.isEmpty) {
      return true
    }
    false
  }

  def md5(value: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    val text = value
    digest.digest(text.getBytes).map("%02x".format(_)).mkString
  }
}
