package streaming

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Json {
  self: Product =>
  import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
  import com.fasterxml.jackson.annotation.PropertyAccessor
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true)

  def fromAnyObjectWithoutPrettyJson[A <: Product](req: A): String = fromAnyObject(req, false)

  protected def fromAnyObject[A <: Product](req: A, prettyJson: Boolean = true): String = {
    if (prettyJson) mapper.writerWithDefaultPrettyPrinter.writeValueAsString(req) else mapper.writeValueAsString(req)
  }
}

trait JsonLike {
  self: Product =>

  def toJson = Json.fromAnyObjectWithoutPrettyJson(self)
}