package com.etiantian.common.param

trait EnvironmentalKey {
  //测试环境上的测试库,es是正式环境
  val HBASE_ZOOKEEPER = "10.2.5.132,10.2.5.133,10.2.5.134"
  val KAFKA_ZOOKEEPER = "cdh132:2181,cdh133:2181,cdh134:2181"
  val BROKER = "cdh132:9092,cdh133:9092,cdh134:9092"
  val GROUP_ID = "test"
  val TOPIC = "ecs5213.aliutfora.tol_j_stu_question,ecs5213.school.stu_paper_ques_logs,ecs5213.school.tp_ques_answer_record,ecs5213.school.br_user_ques_log"
  val STU_TOPIC = "ecs5213.school.stu_paper_ques_logs"
  val ANS_TOPIC = "ecs5213.school.tp_ques_answer_record"
  val BR_TOPIC = "ecs5213.school.br_user_ques_log"
  val TOLJ_TOPIC = "ecs5213.aliutfora.tol_j_stu_question"
  val COMMON_TOPIC = "ecs5213.school.user_info"
  val TABLE_COLUMN = "user_id,ref,ett_user_id"
  val TABLE_KEY = "user_id|ref"
  val DATABASE = "ecs5213"
  val CLUSTER_NAME = "espro"
  val MAX_ACTION = "100000"
  val ES_NAME = "10.1.5.221,10.1.5.222,10.1.5.223"
  val ES_PORT = 9300
  val ES_INDEX = "his_behavior_trace_w"
  val ES_TYPE = "history"

  //t45,hbase和es为正式库
//  val HBASE_ZOOKEEPER = "t193,t194,t195"
//  val KAFKA_ZOOKEEPER = "t45.test.etiantian.com:2181"
//  val BROKER = "192.168.10.45:9092"
//  val GROUP_ID = "test1"
//  //t45.test.stu_paper_ques_logs,t45.test.tp_ques_answer_record,t45.test.br_user_ques_log
//  val TOPIC = "t45.test.tol_j_stu_question"
//  val STU_TOPIC = "t45.test.stu_paper_ques_logs"
//  val ANS_TOPIC = "t45.test.tp_ques_answer_record"
//  val BR_TOPIC = "t45.test.br_user_ques_log"
//  val TOLJ_TOPIC = "t45.test.tol_j_stu_question"
//  val COMMON_TOPIC = "t45.test.user_info"
//  val DATABASE = "t45"
//  val CLUSTER_NAME = "espro"
//  val MAX_ACTION = "1000"
//  val ES_NAME = "gw1in.aliyun.etiantian.net"
//  val ES_PORT = 12213
//  val ES_INDEX = "behavior_info_flink1"
//  val ES_TYPE = "info"
}