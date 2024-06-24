package saft


import zio.*
import zio.json.*

import zio.json.internal.Write

private trait JsonCodecs {
  given JsonDecoder[Term] = JsonDecoder[Int].map(Term(_))
  given JsonEncoder[Term] = JsonEncoder[Int].contramap(identity)
  given JsonDecoder[LogIndex] = JsonDecoder[Int].map(LogIndex(_))
  given JsonEncoder[LogIndex] = JsonEncoder[Int].contramap(identity)
  given JsonDecoder[LogData] = JsonDecoder[String].map(LogData(_))
  given JsonEncoder[LogData] = JsonEncoder[String].contramap(identity)
  given JsonDecoder[NodeId] = JsonDecoder[Int].map(NodeId.apply)
  given JsonEncoder[NodeId] = JsonEncoder[Int].contramap(_.number)
  given JsonDecoder[LogIndexTerm] = DeriveJsonDecoder.gen[LogIndexTerm]
  given JsonEncoder[LogIndexTerm] = DeriveJsonEncoder.gen[LogIndexTerm]
  given JsonDecoder[LogEntry] = DeriveJsonDecoder.gen[LogEntry]
  given JsonEncoder[LogEntry] = DeriveJsonEncoder.gen[LogEntry]
  given JsonDecoder[AppendEntries] = DeriveJsonDecoder.gen[AppendEntries]
  given JsonEncoder[AppendEntries] = DeriveJsonEncoder.gen[AppendEntries]
  given JsonDecoder[AppendEntriesResponse] = DeriveJsonDecoder.gen[AppendEntriesResponse]
  given JsonEncoder[AppendEntriesResponse] = DeriveJsonEncoder.gen[AppendEntriesResponse]
  given JsonDecoder[RequestVote] = DeriveJsonDecoder.gen[RequestVote]
  given JsonEncoder[RequestVote] = DeriveJsonEncoder.gen[RequestVote]
  given JsonDecoder[RequestVoteResponse] = DeriveJsonDecoder.gen[RequestVoteResponse]
  given JsonEncoder[RequestVoteResponse] = DeriveJsonEncoder.gen[RequestVoteResponse]
  given JsonDecoder[NewEntry] = DeriveJsonDecoder.gen[NewEntry]
  given JsonEncoder[NewEntry] = DeriveJsonEncoder.gen[NewEntry]
  given JsonDecoder[NewEntryAddedSuccessfullyResponse.type] = DeriveJsonDecoder.gen[NewEntryAddedSuccessfullyResponse.type]
  given JsonEncoder[NewEntryAddedSuccessfullyResponse.type] = DeriveJsonEncoder.gen[NewEntryAddedSuccessfullyResponse.type]
  given JsonDecoder[RedirectToLeaderResponse] = DeriveJsonDecoder.gen[RedirectToLeaderResponse]
  given JsonEncoder[RedirectToLeaderResponse] = {
    val delegate = JsonEncoder.option(summon[JsonEncoder[NodeId]])
    given JsonEncoder[Option[NodeId]] = (a: Option[NodeId], indent: Option[Int], out: Write) => delegate.unsafeEncode(a, indent, out)
    DeriveJsonEncoder.gen[RedirectToLeaderResponse]
  }
  given JsonDecoder[NewEntryFromFollowerResponse] = DeriveJsonDecoder.gen[NewEntryFromFollowerResponse]
  given JsonEncoder[NewEntryFromFollowerResponse] = DeriveJsonEncoder.gen[NewEntryFromFollowerResponse]
  given JsonDecoder[NewEntryFromFollower] = DeriveJsonDecoder.gen[NewEntryFromFollower]
  given JsonEncoder[NewEntryFromFollower] = DeriveJsonEncoder.gen[NewEntryFromFollower]

  def encodeResponse(r: ResponseMessage): String = r match
    case r: RequestVoteResponse                    => r.toJson
    case r: AppendEntriesResponse                  => r.toJson
    case r: NewEntryAddedSuccessfullyResponse.type => r.toJson
    case r: RedirectToLeaderResponse               => r.toJson
    case r: NewEntryFromFollowerResponse           => r.toJson

  def encodeRequest(m: RequestMessage): String = m match
    case r: RequestVote   => r.toJson
    case r: AppendEntries => r.toJson
    case r: NewEntry      => r.toJson
    case r: NewEntryFromFollower => r.toJson

  def decodeResponse(toRequest: RequestMessage with FromServerMessage, data: String): Either[String, ResponseMessage with ToServerMessage] =
    toRequest match
      case _: RequestVote   => data.fromJson[RequestVoteResponse]
      case _: AppendEntries => data.fromJson[AppendEntriesResponse]
      case _: NewEntryFromFollower => data.fromJson[NewEntryFromFollowerResponse]
}