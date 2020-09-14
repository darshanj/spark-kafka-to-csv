package streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import streaming.config.{CDCConfig, Config}

import scala.collection.mutable



object SaveCDCMessages {
  def main(args: Array[String]): Unit = {
    val config = CDCConfig(args)
    val spark = SparkSession
      .builder()
      .appName("SaveCDCMessages")
      .master(config.sparkMasterUrl)
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.session.timeZone", TimeZoneUTC.getID)
    save(config, KafkaReader(config))
  }

  def save(config: Config, reader: Reader): StreamingQuery = {
    reader
      .read
      .selectValue
      .dropNulls
      .withTableColumn.dataStream.writeStream(config)
  }

  val progress: InMemoryQueryProgressListener = InMemoryQueryProgressListener()
  def startTracking(): InMemoryQueryProgressListener = {
    val spark = SparkSession.getActiveSession.get
    spark.streams.addListener(progress)
    progress
  }

  trait QueryTrackingLike {
    val id: UUID
    val start: QueryStartedEvent
    val progress: mutable.Queue[StreamingQueryProgress]
    val end: QueryTerminatedEvent
  }

  case class InMemoryQueryProgressListener(queries :mutable.Map[UUID,QueryTrackingLike] = mutable.Map.empty) extends StreamingQueryListener {

    object QueryTracking {
      trait QueryStarted  extends QueryTrackingLike {
        self => QueryTrackingStarted
        def addProgress(event: QueryProgressEvent) : QueryTrackingLike
        def terminated(event: QueryTerminatedEvent): QueryTrackingLike
      }

      case class QueryTrackingStarted(start: QueryStartedEvent,queryProgress : mutable.Queue[StreamingQueryProgress] = mutable.Queue.empty[StreamingQueryProgress]) extends QueryStarted {

        override def addProgress(event: QueryProgressEvent): QueryTrackingLike =  {
          queryProgress.synchronized { queryProgress += event.progress }
          QueryTrackingStarted(start,queryProgress)
        }

        override def terminated(event: QueryTerminatedEvent): QueryTrackingLike = {
          QueryTrackingEnded(start,queryProgress,event)
        }

        override val progress: mutable.Queue[StreamingQueryProgress] = queryProgress.clone()
        override val end: QueryTerminatedEvent = throw new UnsupportedOperationException("Query still not ended")
        override val id: UUID = start.id
      }
      case class QueryTrackingEnded(start: QueryStartedEvent,progress: mutable.Queue[StreamingQueryProgress], end: QueryTerminatedEvent) extends QueryTrackingLike {
        override val id: UUID = start.id
      }

      def start(start: QueryStartedEvent): QueryTrackingStarted with QueryStarted = QueryTrackingStarted(start)
    }


    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println("Query started: " + queryStarted.id)
      queries(queryStarted.id) = QueryTracking.start(queryStarted)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + queryTerminated.id)
      queries(queryTerminated.id) = queries(queryTerminated.id) match {
        case qs: QueryTracking.QueryStarted =>
          qs.terminated(queryTerminated)
        case _ => throw new UnsupportedOperationException("Query still not ended")
      }
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      queries(queryProgress.progress.id) = queries(queryProgress.progress.id) match {
        case qs: QueryTracking.QueryStarted =>
          qs.addProgress(queryProgress)
        case _ => throw new UnsupportedOperationException("Query still not ended")
      }
      println("Query made progress: " + queryProgress.progress)
    }
  }

}
