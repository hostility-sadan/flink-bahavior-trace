package com.etiantian.comom.util

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.json.JsonXContent

class WriteEsData {

  def addEs(data: String): (String,XContentBuilder)={
      val array = data.toString.split("&")
      val content = JsonXContent.contentBuilder().startObject()
        .field("id", array(0))
        .field("jid", array(1))
        .field("object_id", array(2))
        .field("answer", array(3))
        .field("c_time", array(4))
        .field("task_id", array(5))
        .field("is_right", array(6))
        .field("subject", array(7))
        .field("object_type", array(10))
        .field("is_axp", array(11))
      if (array(8) != "null" && array(9) != "") {
        content.field("point_ids", array(8))
        content.field("list", array(9))
      }
      else if ((array(8) == "null" || array(8) == "") && array(9) != "" && array(9) != "null") {
        content.field("list", array(9))
      }
      else { //(array(8) != "null" && array(8) != "" && (array(9) == "" || array(9) == "null"))
        content.field("point_ids", array(8))
      }
     content.endObject()
      (array(0),content)
  }

}
