package saft

private enum NodeRole:
  def state: ServerState
  def timer: Timer

  case Follower(state: ServerState, followerState: FollowerState, timer: Timer) extends NodeRole
  case Candidate(state: ServerState, candidateState: CandidateState, timer: Timer) extends NodeRole
  case Leader(state: ServerState, leaderState: LeaderState, timer: Timer) extends NodeRole
  