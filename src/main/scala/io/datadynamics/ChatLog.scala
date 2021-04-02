package io.datadynamics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ChatLog {
  def create(line: String): ChatLog = {
    val splits: Array[String] = line.split("\t")
    val bjId: String = splits(Chat.bj_id.id)
    val bStartTime: Long = splits(Chat.b_start_time.id).toLong
    val bTitle: String = splits(Chat.b_title.id)
    val userId: String = splits(Chat.user_id.id)
    val userNick: String = splits(Chat.user_nick.id)
    val chatText: String = splits(Chat.chat_text.id)
    val chatNow: Long = splits(Chat.chat_now.id).toLong
    ChatLog(bjId, bStartTime, bTitle, userId, userNick, chatText, chatNow)
  }
}

case class ChatLog(bjId: String, bStartTime:Long, bTitle: String, userId: String, userNick: String, chatText: String, chatNow: Long)

object Chat extends Enumeration {
  val bj_id, bj_name, b_title, b_start_time, all_viewer, pc_viewer, mobile_viewer, relay_viewer, total_viewer, user_titles, user_id, login_index, user_nick, is_mobile, userflag, grade, chat_text, chat_now, version, up_count = Value
}