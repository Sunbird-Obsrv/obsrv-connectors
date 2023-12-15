package org.sunbird.obsrv.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JSONUtil {

  @transient private val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)
    .build()

  mapper.setSerializationInclusion(Include.NON_NULL)

  def serialize(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }

  def deserialize[T](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz);
  }

}
