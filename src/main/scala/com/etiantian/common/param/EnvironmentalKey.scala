package com.etiantian.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = "t193,t194,t195"
  val KAFKA_ZOOKEEPER = "t45.test.etiantian.com:2181"
  val BROKER = "192.168.10.45:9092"
  val GROUP_ID = "test1"
  //t45.test.stu_paper_ques_logs,t45.test.tp_ques_answer_record,t45.test.br_user_ques_log
  val TOPIC = "t45.test.stu_paper_ques_logs,t45.test.tp_ques_answer_record,t45.test.br_user_ques_log"
  val STU_TOPIC = "t45.test.stu_paper_ques_logs"
  val ANS_TOPIC = "t45.test.tp_ques_answer_record"
  val BR_TOPIC = "t45.test.br_user_ques_log"
  val COMMON_TOPIC = "t45.test.user_info"
  val CLUSTER_NAME = "espro"
  val MAX_ACTION = "1000"
  val ES_NAME = "gw1in.aliyun.etiantian.net"
  val ES_PORT = 12213
  val ES_INDEX = "behavior_info_flink"
  val ES_TYPE = "info"
}