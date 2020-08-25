package streaming
case class CDCMessage(topic:String, tableName:String, record:String, source_ts:String, ts_ms:String) extends JsonLike
case class TestRecord(a: String, b: Int) extends JsonLike