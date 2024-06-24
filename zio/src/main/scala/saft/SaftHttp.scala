package saft
package saft.http.JsonCodecs

import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio.*
import zio.json.*
import zhttp.http.*
import zhttp.service.Server

import java.nio.file.{Files, Path as JPath}

object SaftHttp1 extends SaftHttp(1, JPath.of("saft1.json"))
object SaftHttp2 extends SaftHttp(2, JPath.of("saft2.json"))
object SaftHttp3 extends SaftHttp(3, JPath.of("saft3.json"))



/** A Raft implementation using json-over-http for inter-node communication and json file-based persistence. */
class SaftHttp(nodeNumber: Int, persistencePath: JPath) extends ZIOAppDefault with JsonCodecs with Logging {
  override val run: Task[Nothing] =
    // configuration
    val conf = Conf.default(3)
    val clientTimeout = Duration.fromSeconds(5)
    val applyLogData = (nodeId: NodeId) => (data: LogData) => setNodeLogAnnotation(nodeId) *> ZIO.log(s"Apply: $data")

    // setup node
    val nodeId = NodeId(nodeNumber)
    def nodePort(nodeId: NodeId): Int = 8080 + nodeId.number
    val port = nodePort(nodeId)

    for {
      stateMachine <- StateMachine.background(applyLogData(nodeId))
      persistence = new FileJsonPersistence(persistencePath)
      queue <- Queue.sliding[ServerEvent](16)
      backend <- HttpClientZioBackend()
      comms = new HttpComms(queue, backend, clientTimeout, nodePort)
      node = new Node(nodeId, comms, stateMachine, conf, persistence)
      _ <- ZIO.log(s"Starting SaftHttp on localhost:$port")
      _ <- ZIO.log(s"Configuration: ${conf.show}")
      _ <- node.start.fork
      result <- Server.start(port, app(queue))
    } yield result

  private def app(queue: Queue[ServerEvent]): HttpApp[Any, Throwable] = Http
    .collectZIO[Request] {
      case r @ Method.POST -> !! / EndpointNames.AppendEntries => decodingEndpoint(r, _.fromJson[AppendEntries], queue)
      case r @ Method.POST -> !! / EndpointNames.RequestVote   => decodingEndpoint(r, _.fromJson[RequestVote], queue)
      case r @ Method.POST -> !! / EndpointNames.NewEntry      => decodingEndpoint(r, _.fromJson[NewEntry], queue)
      case r @ Method.POST -> !! / EndpointNames.NewEntryFromFollower => decodingEndpoint(r, _.fromJson[NewEntryFromFollower], queue)
    }
    .catchAll {
      case e: TimedOutException => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.RequestTimeout)))
      case e: DecodeException   => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.BadRequest)))
      case e: Exception         => Http.fromZIO(ZIO.logErrorCause(Cause.fail(e)).as(Response(Status.InternalServerError)))
    }

  private def decodingEndpoint[T <: RequestMessage with ToServerMessage](
      request: Request,
      decode: String => Either[String, T],
      queue: Queue[ServerEvent]
  ): Task[Response] =
    request.body.asString.map(decode).flatMap {
      case Right(msg) =>
        for {
          p <- Promise.make[Nothing, ResponseMessage]
          _ <- queue.offer(ServerEvent.RequestReceived(msg, r => p.succeed(r).unit))
          r <- p.await.timeoutFail(TimedOutException(s"Handling request message: $msg"))(Duration.fromSeconds(1))
        } yield Response.text(encodeResponse(r))
      case Left(errorMsg) => ZIO.fail(DecodeException(s"Handling request message: $errorMsg"))
    }
}


private class FileJsonPersistence(path: JPath) extends Persistence with JsonCodecs {
  private given JsonDecoder[ServerState] = DeriveJsonDecoder.gen[ServerState]
  private given JsonEncoder[ServerState] = DeriveJsonEncoder.gen[ServerState]

  override def apply(oldState: ServerState, newState: ServerState): UIO[Unit] =
    ZIO
      .attempt {
        val json = newState.copy(commitIndex = None, lastApplied = None).toJsonPretty
        Files.writeString(path, json)
      }
      .unit
      .orDie

  override def get: UIO[ServerState] =
    if path.toFile.exists()
    then ZIO.fromEither(Files.readString(path).fromJson[ServerState]).mapError(DecodeException.apply).orDie
    else ZIO.succeed(ServerState.Initial)
}
