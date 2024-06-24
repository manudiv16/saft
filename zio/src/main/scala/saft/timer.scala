package saft

import zio.{Exit, Fiber, UIO, ZIO}


/** @param currentTimer
  *   The currently running timer - a fiber, which eventually adds a [[Timeout]] event using [[comms.add]]. That fiber can be interrupted to
  *   cancel the timer.
  */
private class Timer(conf: Conf, comms: Comms, currentTimer: Fiber.Runtime[Nothing, Unit]):
  private def restart(timeout: UIO[ServerEvent.Timeout.type]): UIO[Timer] =
    for {
      _ <- currentTimer.interrupt
      newFiber <- timeout.flatMap(comms.add).fork
    } yield new Timer(conf, comms, newFiber)
  def restartElection: UIO[Timer] = restart(conf.electionTimeout)
  def restartHeartbeat: UIO[Timer] = restart(conf.heartbeatTimeout)

private object Timer:
  def apply(conf: Conf, comms: Comms): UIO[Timer] = ZIO.never.fork.map(new Timer(conf, comms, _))
