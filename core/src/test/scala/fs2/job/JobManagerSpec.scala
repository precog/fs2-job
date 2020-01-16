/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2
package job

import scala.Predef._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Either, Left, Right}
import scala.{Int, List, Unit}

import java.lang.Exception

import cats.Eq
import cats.effect.{IO, Resource, Timer}
import cats.instances.string._
import fs2.concurrent.SignallingRef
import org.specs2.mutable._
import org.specs2.execute.AsResult
import org.specs2.specification.core.Fragment

object JobManagerSpec extends Specification {
  implicit val cs = IO.contextShift(ExecutionContext.global)
  implicit val timer = IO.timer(ExecutionContext.global)

  // two durations are considered equal if they're within Delta of each other
  val Delta = 50L

  // how long we sleep to simulate work in streams
  val WorkingTime = (Delta * 10).milliseconds

  // how long we wait for EACH test to finish. This is important since latchGet may block indefinitely.
  val Timeout = WorkingTime * 10

  implicit class RunExample(s: String) {
    def >>*[A: AsResult](io: => IO[A]): Fragment =
      s >> io.timeout(Timeout).unsafeRunSync
  }

  def await: Stream[IO, Unit] =
    Stream.sleep(WorkingTime)

  def sleep: IO[Unit] = 
    IO.sleep(WorkingTime)

  def jobManager: Resource[IO, JobManager[IO, Int, String]] =
    JobManager[IO, Int, String]()

  // blocks until s.get === expected
  def latchGet(s: SignallingRef[IO, String], expected: String): IO[Unit] =
    s.discrete.filter(Eq[String].eqv(_, expected)).take(1).compile.drain

  "Job manager" should {
    "submit a job" >>* {
      val JobId = 1

      def jobStream(ref: SignallingRef[IO, String]): Stream[IO, Either[String, Int]] =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          submitResult <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Started")
          ids <- mgr.jobIds
          status <- mgr.status(JobId)
        } yield {
          submitResult must beTrue
          ids must_== List(JobId)
          status must beSome(Status.Running)
        }
      }
    }
  
    "execute a job to completion" >>* {
      val JobId = 42

      def jobStream(ref: SignallingRef[IO, String]): Stream[IO, Either[String, Int]] =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          job = Job(JobId, jobStream(ref))

          submitStatus <- mgr.submit(job)
          _ <- latchGet(ref, "Started")
          refAfterStart <- ref.get

          _ <- latchGet(ref, "Finished")
          refAfterRun <- ref.get
        } yield {
          refAfterStart mustEqual "Started"
          refAfterRun mustEqual "Finished"
        }
      }
    }

    "cancel a job" >>* {
      val JobId = 42

      def jobStream(ref: SignallingRef[IO, String]): Stream[IO, Either[String, Int]] =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Working")).as(Right(3)) ++ await.as(Right(4)) ++ Stream.eval(ref.set("Finished")).as(Right(5))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          job = Job(JobId, jobStream(ref))

          _ <- mgr.submit(job)
          _ <- latchGet(ref, "Started")

          statusBeforeCancel <- mgr.status(JobId)
          refBeforeCancel <- ref.get

          _ <- latchGet(ref, "Working")
          _ <- mgr.cancel(JobId)
          _ <- sleep

          refAfterCancel <- ref.get
        } yield {
          statusBeforeCancel must beSome(Status.Running)
          refBeforeCancel mustEqual "Started"
          refAfterCancel mustEqual "Working"
        }
      }
    }

    "emit notifications" >>* {
      val JobId = 42

      val jobStream: Stream[IO, Either[String, Int]] =
        Stream(Right(1), Right(2), Left("50%"), Right(3), Right(4), Left("100%")).covary[IO]

      jobManager use { mgr =>
        for {
          submitStatus <- mgr.submit(Job(JobId, jobStream))
          _ <- sleep
          ns <- mgr.lastNotifications(2)
        } yield ns must beSome(List((JobId, "50%"), (JobId, "100%")))
      }
    }

    "tapped jobs can be canceled" >>* {
      val JobId = 42

      def jobStream(ref: SignallingRef[IO, String]): Stream[IO, Either[String, Int]] =
        await.as(Right(1)) ++ Stream.eval(ref.set("Started")).as(Right(2)) ++ await.as(Right(3)) ++ Stream.eval(ref.set("Finished")).as(Right(4))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          tappedStream = mgr.tap(Job(JobId, jobStream(ref))).fold(List[Int]()) {
            case (acc, elem) => acc :+ elem
          }
          // sequence tapped stream manually
          _ <- tappedStream.concurrently(Stream.eval(latchGet(ref, "Started")) ++ Stream.eval(mgr.cancel(JobId)))
                .compile
                .drain
          results <- ref.get
        } yield results mustEqual "Started"
      }
    }

    "not crash when a submitted job crashes" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      val failingJobStream =
        Stream.emit(Right(1)).covary[IO] ++ Stream.raiseError[IO](new Exception("boom")).as(Right(2)) ++ Stream.emit(Right(3)).covary[IO]

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(1, jobStream(ref)))
          _ <- mgr.submit(Job(2, failingJobStream))
          _ <- latchGet(ref, "Finished")
          results <- ref.get
        } yield results mustEqual "Finished"
      }
    }

    "not crash when a tapped job crashes" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      val failingJobStream =
        Stream.emit(Right(1)).covary[IO] ++ Stream.raiseError[IO](new Exception("boom")).as(Right(2)) ++ Stream.emit(Right(3)).covary[IO]

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(1, jobStream(ref)))
          // boom
          _ <- mgr.tap(Job(2, failingJobStream)).compile.drain
          _ <- latchGet(ref, "Finished")
          results <- ref.get
        } yield results mustEqual "Finished"
      }
    }

    "report errors when a job crashes" >>* {
      val JobId = 1

      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.raiseError[IO](new Exception("boom")) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Started")
          event <- mgr.events.take(1).compile.lastOrError
        } yield event must beLike {
          case Event.Failed(i, _, _, throwable) => {
            i mustEqual JobId
            throwable.getMessage mustEqual "boom"
          }
        }
      }
    }

    "report when a job completes" >>* {
      val JobId = 1

      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.eval(ref.set("Finished")).as(Right(2))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Finished")
          event <- mgr.events.take(1).compile.lastOrError
        } yield event must beLike {
          case Event.Completed(i, _, _) => i mustEqual JobId
        }
      }
    }

    "report both failing and completing jobs" >>* {
      val JobId = 1
      val FailingJobId = 2

      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      val failingJobStream =
        Stream.emit(Right(1)).covary[IO] ++ Stream.raiseError[IO](new Exception("boom")).as(Right(2)) ++ Stream.emit(Right(3)).covary[IO]

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- mgr.submit(Job(FailingJobId, failingJobStream))
          _ <- latchGet(ref, "Finished")
          events <- mgr.events.take(2).fold(List[Event[Int]]())({
            case (acc, elem) => acc :+ elem
          }).compile.lastOrError
        } yield {
          events must have size(2)
          events must contain((event: Event[Int]) => event must beLike {
            case Event.Completed(i, _, _) => i mustEqual JobId
          }).exactly(1.times)

          events must contain((event: Event[Int]) => event must beLike {
            case Event.Failed(i, _, _, ex) =>
              i mustEqual FailingJobId
              ex.getMessage mustEqual "boom"
          }).exactly(1.times)
        }
      }
    }

    "report duration when completing a job" >>* {
      val JobId = 1

      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Working")).as(Right(3)) ++ Stream.raiseError[IO](new Exception("boom")).as(Right(4))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Started")
          startTime <- Timer[IO].clock.realTime(MILLISECONDS)
          _ <- latchGet(ref, "Working")
          endTime <- Timer[IO].clock.realTime(MILLISECONDS)
          event <- mgr.events.take(1).compile.lastOrError
        } yield {
          event must beLike {
            case Event.Failed(id, _, duration, ex) =>
              ex.getMessage mustEqual "boom"
              duration.toMillis must beCloseTo(endTime - startTime, Delta)
              id mustEqual JobId
          }
        }
      }
    }

    "report duration when a job fails" >>* {
      val JobId = 1

      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Started")
          startTime <- Timer[IO].clock.realTime(MILLISECONDS)
          _ <- latchGet(ref, "Finished")
          endTime <- Timer[IO].clock.realTime(MILLISECONDS)
          event <- mgr.events.take(1).compile.lastOrError
        } yield {
          event must beLike {
            case Event.Completed(id, _, duration) =>
              duration.toMillis must beCloseTo(endTime - startTime, Delta)
              id mustEqual JobId
          }
        }
      }
    }

    "shutdown even when event queue is full" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await.as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      // eventsLimit = 2
      JobManager[IO, Int, String](10, 10, 2) use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")

          // submit job once
          _ <- mgr.submit(Job(1, jobStream(ref)))
          _ <- latchGet(ref, "Finished")

          ref2 <- SignallingRef[IO, String]("Not started")

          // submit again to fill event queue
          _ <- mgr.submit(Job(2, jobStream(ref2)))
          _ <- latchGet(ref2, "Finished")
        } yield ok
      }
    }

    "shutdown even when notification queue is full" >>* {
      val JobId = 1

      // emit exactly 2 notifications
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.emit(Left("50%")).covary[IO] ++ await.as(Right(2)) ++ Stream.emit(Left("100%")).covary[IO] ++ Stream.eval(ref.set("Finished")).as(Right(3))

      // notificationsLimit = 2
      JobManager[IO, Int, String](10, 2, 10) use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(JobId, jobStream(ref)))
          _ <- latchGet(ref, "Finished")
        } yield ok
      }
    }

    "report unfiltered notifications" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.emit(Left("50%")).covary[IO] ++ await.as(Right(2)) ++ Stream.emit(Left("100%")).covary[IO] ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref1 <- SignallingRef[IO, String]("Not started")
          ref2 <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(1, jobStream(ref1)).silent)
          _ <- mgr.submit(Job(2, jobStream(ref2)))
          _ <- latchGet(ref1, "Finished")
          _ <- latchGet(ref2, "Finished")
          ns <- mgr.lastNotifications(2)
        } yield ns must beSome(List((2, "50%"), (2, "100%")))
      }
    }

    "not report filtered notifications" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.emit(Left("50%")).covary[IO] ++ await.as(Right(2)) ++ Stream.emit(Left("100%")).covary[IO] ++ Stream.eval(ref.set("Finished")).as(Right(3))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(1, jobStream(ref)).silent)
          _ <- latchGet(ref, "Finished")
          ns <- mgr.lastNotifications(1)
        } yield ns must beNone
      }
    }

    "retain the effects of filtered notifications" >>* {
      def jobStream(ref: SignallingRef[IO, String]) =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ Stream.emit(Right(2)).covary[IO] ++ Stream.eval(ref.set("Finished")).as(Left("100%"))

      jobManager use { mgr =>
        for {
          ref <- SignallingRef[IO, String]("Not started")
          _ <- mgr.submit(Job(1, jobStream(ref)).silent)
          // this would never complete if the last notification's effect was discarded
          _ <- latchGet(ref, "Finished")
        } yield ok
      }
    }
  }
}
