package streaming


// {
//    "id":999,
//    "item":null,
//    "qty":null,
//    "__op":"d",
//    "__name":"postgresphdhqaairflow",
//    "__table":"inventory",
//    "__lsn":139234131816,
//    "__txId":2673501,
//    "__source_ts_ms":1598596786555
//    ,"__source_schema":"public",
//    "__ts_ms":1598596786999,
//    "__deleted":"true"
//}

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[TableOne], name = "tableOne"),
  new Type(value = classOf[TableTwo], name = "tableTwo")
))
abstract case class CDCMessageMetadata(__op:String,
                                        __name:String,
                                        __table:String,
                                        __lsn:Long,
                                        __txId:Long,
                                        __source_ts_ms:Long,
                                        __source_schema:String,
                                        __ts_ms:Long,
                                        __deleted:String) extends JsonLike

class TableOne(a: String, b: Int, c:String) extends CDCMessageMetadata("c","name","t1",12,23,0,"ss",0,"true")
class TableTwo(b: String, c: Int, d:String) extends CDCMessageMetadata("c","name","t2",12,23,0,"ss",0,"true")
