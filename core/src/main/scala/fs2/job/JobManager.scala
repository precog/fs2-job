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

import cats.syntax.flatMap._
import cats.syntax.functor._

import cats.effect.{Concurrent, Timer}

import fs2.concurrent.{Queue, SignallingRef}

import scala.{Array, Boolean, Int, List, Option, None, Nothing, Predef, Some, Unit}, Predef.???
import scala.collection.JavaConverters._
import scala.concurrent.duration._

import java.lang.{RuntimeException, SuppressWarnings}
import java.util.concurrent.ConcurrentHashMap

final class JobManager[F[_]: Concurrent: Timer, I, N] private (
    notificationsQ: Queue[F, Option[N]],
    dispatchQ: Queue[F, Stream[F, Nothing]]) {

  import JobManager._

  private[this] val meta: ConcurrentHashMap[I, Context[F]] = new ConcurrentHashMap[I, Context[F]]

  val notifications: Stream[F, N] = notificationsQ.dequeue.unNoneTerminate

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments", "org.wartremover.warts.Equals"))
  def submit[R](
      job: Job[F, I, N, R],
      delay: Option[FiniteDuration] = None)
      : F[Boolean] = {

    val run = job.run
      .map(_.swap.toOption)
      .unNone
      .map(Some(_))
      .evalMap(notificationsQ.enqueue1)
      .drain

    val putStatusF = Concurrent[F] delay {
      val attempt = Context[F](Status.Pending, None)
      attempt eq meta.putIfAbsent(job.id, attempt)
    }

    putStatusF flatMap { s =>
      if (s)
        dispatchQ.enqueue1(managementMachinery(job.id, run)).as(true)
      else
        Concurrent[F].pure(false)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def schedule[R](
      job: Job[F, I, N, R],
      period: FiniteDuration,
      delay: Option[FiniteDuration] = None)
      : F[Unit] = ???

  def tap[R](job: Job[F, I, N, R]): Stream[F, R] =
    managementMachinery(job.id, job.run.map(_.toOption).unNone)

  def jobIds: F[List[I]] =
    Concurrent[F].delay(meta.keys.asScala.toList)

  def cancel(id: I): F[Unit] = {
    Concurrent[F].delay(meta.get(id)) flatMap {
      case Context(Status.Running, Some(cancelF)) => cancelF
      case _ => Concurrent[F].unit
    }
  }

  def status(id: I): F[Option[Status]] =
    Concurrent[F].delay(Option(meta.get(id)).map(_.status))

  private def shutdown: F[Unit] =
    Concurrent[F].delay(meta.clear()) >> notificationsQ.enqueue1(None)

  private[this] def managementMachinery[A](id: I, in: Stream[F, A]): Stream[F, A] = {
    Stream force {
      SignallingRef[F, Boolean](false) map { s =>
        lazy val frontF: F[Unit] = {
          Concurrent[F].delay(Option(meta.get(id))) flatMap {
            case Some(old @ Context(Status.Pending, _)) =>
              val casF = Concurrent[F] delay {
                meta.replace(id, old, Context(Status.Running, Some(s.set(true))))
              }

              casF flatMap { result =>
                if (result)
                  Concurrent[F].unit
                else
                  frontF
              }

            case Some(Context(Status.Canceled, _)) =>
              Concurrent[F].delay(meta.remove(id)).void

            case Some(Context(Status.Running, _)) =>
              Concurrent[F].raiseError(new RuntimeException("job already running!"))    // TODO

            case None =>
              val casF = Concurrent[F] delay {
                val attempt = Context(Status.Running, Some(s.set(true)))
                attempt eq meta.putIfAbsent(id, attempt)
              }

              casF flatMap { result =>
                if (result)
                  Concurrent[F].unit
                else
                  frontF
              }
          }
        }

        Stream.bracket(frontF)(_ => Concurrent[F].delay(meta.remove(id)).void) >>
          in.interruptWhen(s)
      }
    }
  }
}

object JobManager {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def apply[F[_]: Concurrent: Timer, I, N](
      jobLimit: Int = 100,
      notificationsLimit: Int = 10)   // all numbers are arbitrary, really
      : Stream[F, JobManager[F, I, N]] = {

    for {
      notificationsQ <- Stream.eval(Queue.bounded[F, Option[N]](notificationsLimit))
      dispatchQ <- Stream.eval(Queue.bounded[F, Stream[F, Nothing]](jobLimit))

      initF = Concurrent[F] delay {
        new JobManager[F, I, N](
          notificationsQ,
          dispatchQ)
      }

      jm <- Stream.bracket(initF)(_.shutdown)
      back <- Stream.emit(jm).concurrently(dispatchQ.dequeue.parJoin(jobLimit))
    } yield back
  }

  private final case class Context[F[_]](status: Status, cancel: Option[F[Unit]])
}
