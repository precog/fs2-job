/*
 * Copyright 2014–2018 SlamData Inc.
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

import scala.{Boolean, Int, List, Long, Unit}
import scala.Predef.String
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Either, Left, Right}

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Ref
import cats.effect.IO
import org.specs2.mutable._

object JobManagerSpec extends Specification {
  implicit val cs = IO.contextShift(ExecutionContext.global)
  implicit val timer = IO.timer(ExecutionContext.global)

  def await(l: Long): Stream[IO, Unit] =
    Stream.sleep(FiniteDuration(l, TimeUnit.SECONDS))

  "Job manager" should {
    "submit a job" in {
      val JobId = 1

      val stream = await(1).as(Right(1)) ++ Stream(2, 3, 4).map(Right(_)).covary[IO]
      val notification = Stream("done").map(Left(_)).covary[IO]

      val job = Job[IO, Int, String, Int](JobId, stream ++ notification)

      val (submitResult, ids, status) = (for {
        mgr <- JobManager[IO, Int, String]()
        submitResult <- Stream.eval(mgr.submit(job))
        _ <- await(1)
        ids <- Stream.eval(mgr.jobIds)
        status <- Stream.eval(mgr.status(JobId))
      } yield (submitResult, ids, status)).compile.lastOrError.unsafeRunSync

      submitResult must beTrue
      ids must_== List(JobId)
      status must beSome(Status.Running)
    }

    "executes a job to completion" in {
      val JobId = 42

      def jobStream(ref: Ref[IO, String]): Stream[IO, Either[String, Int]] =
        Stream.eval(ref.set("Started")).as(Right(1)) ++ await(2).as(Right(2)) ++ Stream.eval(ref.set("Finished")).as(Right(3))

      val (refAfterStart, refAfterRun) = (for {
        mgr <- JobManager[IO, Int, String]()
        ref <- Stream.eval(Ref[IO].of("Not started"))
        job = Job[IO, Int, String, Int](JobId, jobStream(ref))

        submitStatus <- Stream.eval(mgr.submit(job))
        _ <- await(1)
        refAfterStart <- Stream.eval(ref.get)
        _ <- await(5)
        refAfterRun <- Stream.eval(ref.get)
      } yield (refAfterStart, refAfterRun)).compile.lastOrError.unsafeRunSync

      refAfterStart mustEqual "Started"
      refAfterRun mustEqual "Finished"
    }

    "cancel a job" in {
      val JobId = 42

      def jobStream(ref: Ref[IO, Boolean]): Stream[IO, Either[String, Int]] =
        await(10).as(Right(1)) ++ Stream.eval(ref.set(true)).as(Right(2))

      val (submitStatus, statusAfterSubmit, statusBeforeCancel, statusAfterCancel, refAfterCancel) = (for {
        mgr <- JobManager[IO, Int, String]()
        ref <- Stream.eval(Ref[IO].of(false))

        job = Job[IO, Int, String, Int](JobId, jobStream(ref))
        submitStatus <- Stream.eval(mgr.submit(job))
        statusAfterSubmit <- Stream.eval(mgr.status(JobId))

        _ <- await(1)
        statusBeforeCancel <- Stream.eval(mgr.status(JobId))

        _ <- Stream.eval(mgr.cancel(JobId))
        _ <- await(1)

        statusAfterCancel <- Stream.eval(mgr.status(JobId))
        refAfterCancel <- Stream.eval(ref.get)
      } yield (submitStatus, statusAfterSubmit, statusBeforeCancel, statusAfterCancel, refAfterCancel)).compile.lastOrError.unsafeRunSync

      refAfterCancel mustEqual false
      submitStatus must beTrue
      statusAfterSubmit must beSome(Status.Pending)
      statusBeforeCancel must beSome(Status.Running)
      statusAfterCancel must beNone
    }
  }
}
