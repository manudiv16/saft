package saft
import sttp.client3.SttpBackend
import zio.*

private case class TimedOutException(msg: String) extends Exception(msg)
private case class DecodeException(msg: String) extends Exception(msg)

private class HttpComms(queue: Queue[ServerEvent], backend: SttpBackend[Task, Any], clientTimeout: Duration, nodePort: NodeId => Int)
    extends Comms
    with JsonCodecs {
  override def next: UIO[ServerEvent] = queue.take
  override def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): UIO[Unit] =
    import sttp.client3.*
    val url = uri"http://localhost:${nodePort(toNodeId)}/${endpoint(msg)}"
    backend
      .send(basicRequest.post(url).body(encodeRequest(msg)).response(asStringAlways))
      .timeoutFail(TimedOutException(s"Client request $msg to $url"))(clientTimeout)
      .map(_.body)
      .flatMap { body =>
        decodeResponse(msg, body) match
          case Left(errorMsg) => ZIO.fail(DecodeException(s"Client request $msg to $url, response: $body, error: $errorMsg"))
          case Right(decoded) => queue.offer(ServerEvent.ResponseReceived(decoded))
      }
      .unit
      .catchAll { case e: Exception => ZIO.logErrorCause(s"Cannot send $msg to $toNodeId", Cause.fail(e)) }
  override def add(event: ServerEvent): UIO[Unit] = queue.offer(event).unit

  private def endpoint(msg: RequestMessage): String = msg match
    case _: AppendEntries => EndpointNames.AppendEntries
    case _: RequestVote   => EndpointNames.RequestVote
    case _: NewEntry      => EndpointNames.NewEntry
    case _: NewEntryFromFollower => EndpointNames.NewEntryFromFollower
}


private object EndpointNames {
  val AppendEntries = "append-entries"
  val RequestVote = "request-vote"
  val NewEntry = "new-entry"
  val NewEntryFromFollower = "new-entry-from-follower"
}