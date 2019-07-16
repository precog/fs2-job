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

import cats.Eq
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import cats.effect.{Concurrent, Timer}

import fs2.concurrent.{Queue, SignallingRef}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Left, Right}
import scala.{Array, Boolean, Int, List, Option, None, Nothing, Some, Unit}

import java.lang.{RuntimeException, SuppressWarnings}
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

/**
 * A coordination mechanism for parallel job management. This
 * structure provides mechanisms for aggregated notifications
 * from jobs, deterministic cancelation, "fire and forget"
 * submission, and sidecar-style cancelation of direct streams.
 */
final class JobManager[F[_]: Concurrent: Timer, I: Eq, N] private (
    notificationsQ: Queue[F, Option[(I, N)]],
    eventQ: Queue[F, Option[Event[I]]],
    dispatchQ: Queue[F, Stream[F, Nothing]]) {

  import JobManager._

  private[this] val meta: ConcurrentHashMap[I, Context[F]] = new ConcurrentHashMap[I, Context[F]]

  val notifications: Stream[F, (I, N)] = notificationsQ.dequeue.unNoneTerminate
  val events: Stream[F, Event[I]] = eventQ.dequeue.unNoneTerminate

  /**
   * Submits a job for parallel execution at the earliest possible moment.
   * If the job submission queue is full, this function will asynchronously
   * block until space is available. Note that a job is visible as Pending
   * immediately upon submission, even when space is unavailable.
   *
   * Attempting to submit a job with the same id as a pre-existing job will
   * produce false.
   */
  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments", "org.wartremover.warts.Equals"))
  def submit[R](job: Job[F, I, N, R]): F[Boolean] = {
    val run = job.run
      .map(_.swap.toOption)
      .unNone
      .tupleLeft(job.id)
      .map(Some(_))
      .evalMap(notificationsQ.enqueue1)
      .drain

    val putStatusF = Concurrent[F] delay {
      val attempt = Context[F](Status.Pending, None)

      Option(meta.putIfAbsent(job.id, attempt)).isEmpty
    }

    putStatusF flatMap { s =>
      if (s)
        Concurrent[F].delay(OffsetDateTime.now()).flatMap(ts =>
          dispatchQ.enqueue1(managementMachinery(job.id, run, ts, false)).as(true))
      else
        Concurrent[F].pure(false)
    }
  }

  /**
   * Like submit, but produces a managed stream equal to the job's
   * run stream. Notifications are stripped and routed to the
   * shared notifications source. The resulting stream is subject
   * to remote cancelation, the same as any submitted job.
   *
   * Attempting to tap a job with a pre-existing id will produce
   * an error.
   */
  def tap[R](job: Job[F, I, N, R]): Stream[F, R] = {
    // TODO failure isn't quite deterministic here when job already exists
    val run = job.run evalMap {
      case Left(n) => notificationsQ.enqueue1(Some((job.id, n))).as(None: Option[R])
      case Right(r) => Concurrent[F].pure(Some(r): Option[R])
    }

    Stream.eval(Concurrent[F].delay(OffsetDateTime.now())).flatMap(ts =>
      managementMachinery(job.id, run.unNone, ts, true))
  }

  /**
   * Returns the currently-running jobs by ID.
   */
  def jobIds: F[List[I]] =
    Concurrent[F].delay(meta.keys.asScala.toList)

   /**
    * Returns the notifications associated with the given job id
    */
   def notificationsOf(id: I): Stream[F, N] =
     notificationsQ.dequeue.unNone.filter {
       case (i, n) => i === id
     }.map(_._2)

  /**
   * Cancels the job by id. If the job does not exist, this call
   * will be ignored.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def cancel(id: I): F[Unit] = {
    Concurrent[F].delay(meta.get(id)) flatMap {
      case Context(Status.Running, Some(cancelF)) =>
        cancelF

      case old @ Context(Status.Pending, _) =>
        val killer = Context[F](Status.Canceled, None)
        Concurrent[F].delay(meta.replace(id, old, killer)) flatMap { success =>
          if (success)
            Concurrent[F].unit
          else
            cancel(id)
        }

      case _ => Concurrent[F].unit
    }
  }

  /**
   * Returns the status of a given job id, if known.
   */
  def status(id: I): F[Option[Status]] =
    Concurrent[F].delay(Option(meta.get(id)).map(_.status))

  private def shutdown: F[Unit] =
    Concurrent[F].delay(meta.clear()) >> notificationsQ.enqueue1(None) >> eventQ.enqueue1(None)

  private[this] def managementMachinery[A](
      id: I,
      in: Stream[F, A],
      startingTime: OffsetDateTime,
      ignoreAbsence: Boolean): Stream[F, A] = {
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
              if (ignoreAbsence) {
                val casF = Concurrent[F] delay {
                  val attempt = Context(Status.Running, Some(s.set(true)))

                  Option(meta.putIfAbsent(id, attempt)).isEmpty
                }

                casF flatMap { result =>
                  if (result)
                    Concurrent[F].unit
                  else
                    frontF
                }
              } else {
                Concurrent[F].unit
              }
          }
        }

        val completeF =
          for {
            ts <- Concurrent[F].delay(OffsetDateTime.now())
            duration = startingTime.until(ts, ChronoUnit.MILLIS).milliseconds
            res <- eventQ.enqueue1(Some(Event.Completed(id, ts, duration)))
          } yield res

        val reported = in ++ Stream.eval_(completeF)

        val handled = reported.handleErrorWith(ex =>
          for {
            ts <- Stream.eval(Concurrent[F].delay(OffsetDateTime.now()))
            duration = startingTime.until(ts, ChronoUnit.MILLIS).milliseconds
            res <- Stream.eval_(eventQ.enqueue1(Some(Event.Failed(id, ts, ex, duration))))
          } yield res)

        Stream.bracket(frontF)(_ => Concurrent[F].delay(meta.remove(id)).void) >> handled.interruptWhen(s)
      }
    }
  }
}

object JobManager {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def apply[F[_]: Concurrent: Timer, I: Eq, N](
      jobLimit: Int = 100,
      notificationsLimit: Int = 10,
      eventsLimit: Int = 10)   // all numbers are arbitrary, really
      : Stream[F, JobManager[F, I, N]] = {

    for {
      notificationsQ <- Stream.eval(Queue.circularBuffer[F, Option[(I, N)]](notificationsLimit))
      eventQ <- Stream.eval(Queue.circularBuffer[F, Option[Event[I]]](eventsLimit))
      dispatchQ <- Stream.eval(Queue.bounded[F, Stream[F, Nothing]](jobLimit))

      initF = Concurrent[F] delay {
        new JobManager[F, I, N](
          notificationsQ,
          eventQ,
          dispatchQ)
      }

      jm <- Stream.bracket(initF)(_.shutdown)
      back <- Stream.emit(jm).concurrently(dispatchQ.dequeue.parJoin(jobLimit))
    } yield back
  }

  private final case class Context[F[_]](status: Status, cancel: Option[F[Unit]])
}
