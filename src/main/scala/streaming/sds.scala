package streaming

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.mutable

class sds {

}

/** Collects events from the StreamingQueryListener for testing */
class EventCollector extends StreamingQueryListener {
  // to catch errors in the async listener events

  @volatile var startEvent: QueryStartedEvent = null
  @volatile var terminationEvent: QueryTerminatedEvent = null

  private val _progressEvents = new mutable.Queue[StreamingQueryProgress]

  def progressEvents: Seq[StreamingQueryProgress] = _progressEvents.synchronized {
    _progressEvents.filter(_.numInputRows > 0).toSeq
  }

  def allProgressEvents: Seq[StreamingQueryProgress] = _progressEvents.synchronized {
    _progressEvents.clone().toSeq
  }

  def reset(): Unit = {
    startEvent = null
    terminationEvent = null
    _progressEvents.clear()
  }

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

      startEvent = queryStarted

  }

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      assert(startEvent != null, "onQueryProgress called before onQueryStarted")
      _progressEvents.synchronized { _progressEvents += queryProgress.progress }
  }

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      assert(startEvent != null, "onQueryTerminated called before onQueryStarted")
      terminationEvent = queryTerminated
  }
}