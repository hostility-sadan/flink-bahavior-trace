package com.etiantian.common.entry

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.json.JSONObject

class JsonDeserializationSchema extends KeyedDeserializationSchema[String] {

  override def isEndOfStream(nextElement: String) = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long) = {
    val json = new JSONObject()
    json.put("topic", topic)
    json.put("partition", partition)
    json.put("offset", offset)
    json.put("key", if (messageKey == null) null else new String(messageKey))
    json.put("value", if (message == null) null else new String(message))
    json.toString()
  }

  override def getProducedType = BasicTypeInfo.STRING_TYPE_INFO
}
