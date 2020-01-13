package com.etiantian.comom.util

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.json.JsonXContent

class WriteEsData {

  def addEs(data: (String, String, String, String, String, String, String, String, String, String, Int, Int,String)): (String, XContentBuilder) = {
    val content = JsonXContent.contentBuilder().startObject()
      .field("id", data._1)
      .field("jid", data._2.toLong)
      .field("object_id", data._3.toLong)
      .field("answer", data._4)
      .field("c_time", data._5)
      .field("is_right", data._7.toInt)
      .field("object_type", data._11)
      .field("is_axp", data._12)
    if (data._8 != "" && data._8 != "null" && data._8 != null){
      content.field("subject_id", data._8.toInt)
    }
    if (data._6 != "" && data._6 != "null" && data._6 != null){
      content.field("task_id", data._6.toLong)
    }
    if (data._9 != "" && data._9 != "null" && data._9 != null){
      content.field("point_ids", data._9)
    }
    if (data._10 != "" && data._10 != "null" && data._10 != null){
      content.field("list", data._10)
    }
    if (data._11 != "" && data._11 != null && data._11 != "null"){
      content.field("dc_school_id", data._11.toLong)
    }

    content.endObject()
    (data._1, content)

    //    val content = JsonXContent.contentBuilder().startObject()
    //      .field("id", array(0))
    //      .field("jid", array(1))
    //      .field("object_id", array(2))
    //      .field("answer", array(3))
    //      .field("c_time", array(4))
    //      .field("task_id", array(5))
    //      .field("is_right", array(6))
    //      .field("subject", array(7))
    //      .field("object_type", array(10))
    //      .field("is_axp", array(11))
    //    if (array(8) != "null" && array(9) != "") {
    //      content.field("point_ids", array(8))
    //      content.field("list", array(9))
    //    }
    //    else if ((array(8) == "null" || array(8) == "") && array(9) != "" && array(9) != "null") {
    //      content.field("list", array(9))
    //    }
    //    else { //(array(8) != "null" && array(8) != "" && (array(9) == "" || array(9) == "null"))
    //      content.field("point_ids", array(8))
    //    }
    //    content.endObject()
    //    (array(0),content)
  }

}
