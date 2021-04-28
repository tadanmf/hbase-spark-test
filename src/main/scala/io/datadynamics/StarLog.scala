package io.datadynamics

object StarLog {
  def create(line: String): StarLog = {
    val splits: Array[String] = line.split("\t")
    val bjId: String = splits(Star.bj_id.id)
    val bStartTime: Long = splits(Star.b_start_time.id).toLong
    val bTitle: String = splits(Star.b_title.id)
    val userId: String = splits(Star.user_id.id)
    val userNick: String = splits(Star.user_nick.id)
    val nowTime: Long = splits(Star.now_time.id).toLong
    val balloonNum: Int = splits(Star.balloon_num.id).toInt
    StarLog(bjId, bStartTime, bTitle, userId, userNick, nowTime, balloonNum)
  }
}

case class StarLog(bjId: String, bStartTime:Long, bTitle: String, userId: String, userNick: String, nowTime:Long, balloonNum:Int)

object Star extends Enumeration {
  val bj_id,bj_name,b_title,b_start_time,all_viewer,pc_viewer,mobile_viewer,relay_viewer,total_viewer,user_id,login_index,user_nick,balloon_num,now_time,version,up_count = Value
}