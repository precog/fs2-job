/*
 * Copyright 2020 Precog Data
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

import cats.effect.{Concurrent, ExitCase, Resource, Timer}
import cats.implicits._

import fs2.concurrent.{Queue, SignallingRef}

import scala.concurrent.duration._
import scala.util.{Left, Right}
import scala.{Boolean, Int, List, Option, None, Nothing, Some, StringContext, Unit}

import java.lang.IllegalStateException
import java.util.concurrent.{ConcurrentHashMap}

/**
 * A coordination mechanism for parallel job management. This
 * structure provides mechanisms for aggregated notifications
 * from jobs, deterministic cancelation, "fire and forget"
 * submission, and sidecar-style cancelation of direct streams.
 */
final class JobManager[F[_]: Concurrent: Timer, I, N] private (
    notificationsQ: Queue[F, Option[(I, N)]],
    eventQ: Queue[F, Option[Event[I]]],
    dispatchQ: Queue[F, Stream[F, Nothing]]) {

  import CompatConverters.All._
  import JobManager._

  private[this] val meta: ConcurrentHashMap[I, Context[F]] = new ConcurrentHashMap[I, Context[F]]

  /**
   * Job notification event stream.
   *
   * If jobs produce notifications, this must be consumed to avoid eventually
   * blocking job progress.
   */
  val notifications: Stream[F, (I, N)] =
    notificationsQ.dequeue.unNoneTerminate

  /**
   * Job termination event stream. It is reentrant w.r.t. the [[JobManager]]
   * and so safe to call [[JobManager]] methods in response to events.
   *
   * This may be safely ignored without affecting job progress if there is no
   * interest in when jobs terminate, however if this is consumed too slowly,
   * events may be missed.
   */
  val events: Stream[F, Event[I]] =
    eventQ.dequeue.unNoneTerminate

  /**
   * Submits a job for parallel execution at the earliest possible moment.
   * If the job submission queue is full, this function will asynchronously
   * block until space is available. Note that a job is visible as Pending
   * immediately upon submission, even when space is unavailable.
   *
   * Attempting to submit a job with the same id as a pre-existing job will
   * produce false.
   */
  def submit[R](job: Job[F, I, N, R]): F[Boolean] = {
    val run = job.run
      .map(_.swap.toOption)
      .unNone
      .tupleLeft(job.id)
      .evalMap(t => notificationsQ.enqueue1(Some(t)))
      .drain

    for {
      isCanceled <- SignallingRef[F, Boolean](false)
      pending = Context[F](Status.Pending, isCanceled.set(true))

      didSubmit <- Concurrent[F].delay {
        Option(meta.putIfAbsent(job.id, pending)).isEmpty
      }

      _ <- if (didSubmit)
        dispatchQ.enqueue1(managementMachinery(job.id, run, isCanceled, false))
      else
        Concurrent[F].unit
    } yield didSubmit
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
    val run = job.run evalMap {
      case Left(n) => notificationsQ.enqueue1(Some((job.id, n))).as(None: Option[R])
      case Right(r) => Concurrent[F].pure(Some(r): Option[R])
    }

    Stream.force(SignallingRef[F, Boolean](false) map { isCanceled =>
      managementMachinery(job.id, run.unNone, isCanceled, true)
    })
  }

  /**
   * Returns the IDs of current jobs.
   */
  def jobIds: F[List[I]] =
    Concurrent[F].delay(meta.keys.asScala.toList)

  /**
   * Returns the last n notifications emitted by jobs. Returns None if
   * notifications are not available.
   */
  def lastNotifications(n: Int): F[Option[List[(I, N)]]] =
    maybeDequeueN(notificationsQ, n)

  /**
   * Returns the last n `Event`s. Returns None if events are
   * not available.
   */
  def lastEvents(n: Int): F[Option[List[Event[I]]]] =
    maybeDequeueN(eventQ, n)

  /**
   * Cancels the job by id. If the job does not exist, this call
   * will be ignored.
   */
  def cancel(id: I): F[Unit] =
    Concurrent[F].delay(meta.get(id)) flatMap {
      case Context(Status.Running, cancel) =>
        cancel

      case old @ Context(Status.Pending, cancel) =>
        (cancel >> Concurrent[F].delay(meta.remove(id, old))) flatMap { success =>
          if (success)
            epochMillisNow
              .map(Event.Canceled(id, _, Duration.Zero))
              .flatMap(e => eventQ.enqueue1(Some(e)))
          else
            Concurrent[F].unit
        }

      case _ => Concurrent[F].unit
    }

  /**
   * Returns the status of a given job id, if known.
   */
  def status(id: I): F[Option[Status]] =
    Concurrent[F].delay(Option(meta.get(id)).map(_.status))

  private def shutdown: F[Unit] = for {
    _ <- Concurrent[F].delay(meta.clear())
    // terminate queues concurrently to always shutdown immediately
    _ <- Concurrent[F].start(notificationsQ.enqueue1(None))
    _ <- Concurrent[F].start(eventQ.enqueue1(None))
  } yield ()

  private def maybeDequeueN[A](q: Queue[F, Option[A]], n: Int): F[Option[List[A]]] =
    q.tryDequeueChunk1(n).map(_.map(_.toList.unite))

  private[this] def managementMachinery[A](
      id: I,
      in: Stream[F, A],
      isCanceled: SignallingRef[F, Boolean],
      ignoreAbsence: Boolean)
      : Stream[F, A] =
    Stream.force(epochMillisNow map { startedAt =>
      // Results in the context expected prior to termination, `None` indicates
      // the job should not be started.
      lazy val attemptF: F[Option[Context[F]]] = {
        val startF: F[Option[Context[F]]] =
          Concurrent[F].delay(Option(meta.get(id))) flatMap {
            // `submit`ted job
            case Some(old @ Context(Status.Pending, cancel)) =>
              val running = Context(Status.Running, cancel)

              val casF = Concurrent[F] delay {
                meta.replace(id, old, running)
              }

              casF flatMap { result =>
                if (result)
                  Concurrent[F].pure(Some(running))
                else
                  attemptF
              }

            case Some(Context(Status.Running, _)) =>
              Concurrent[F].raiseError(new IllegalStateException(s"A job with id '$id' is already running!"))

            // `tap`ped Stream
            case None if ignoreAbsence =>
              val running = Context(Status.Running, isCanceled.set(true))

              val casF = Concurrent[F] delay {
                Option(meta.putIfAbsent(id, running)).isEmpty
              }

              casF flatMap { result =>
                if (result)
                  Concurrent[F].pure(Some(running))
                else
                  attemptF
              }

            // `cancel`ed while Pending
            case None => Concurrent[F].pure(None)
          }

        isCanceled.get.ifM(
          ifTrue = Concurrent[F].pure(None),
          ifFalse = startF)
      }

      def notifyTerminated(f: (Timestamp, FiniteDuration) => Event[I]): F[Unit] =
        for {
          ts <- epochMillisNow
          duration = ts.epoch - startedAt.epoch
          _ <- Concurrent[F].delay(meta.remove(id)).void
          _ <- eventQ.enqueue1(Some(f(startedAt, duration)))
        } yield ()

      val managed =
        (in ++ Stream.eval_(notifyTerminated(Event.Completed(id, _, _))))
          .handleErrorWith(t => Stream.eval_(notifyTerminated(Event.Failed(id, _, _, t))))
          .onFinalizeCase {
            case ExitCase.Canceled => notifyTerminated(Event.Canceled(id, _, _))
            case _ => Concurrent[F].unit
          }

      // Conditional `remove` so we only cleanup if the job is still ours
      Stream.bracket(attemptF)(_.traverse_(c => Concurrent[F].delay(meta.remove(id, c))))
        .flatMap(_.fold[Stream[F, A]](Stream.empty)(_ => managed.interruptWhen(isCanceled)))
    })

  private def epochMillisNow: F[Timestamp] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(FiniteDuration(_, MILLISECONDS))
      .map(Timestamp(_))
}

object JobManager {
  /**
   * Construct a new [[JobManager]] as a `Resource` that, when finalized,
   * interrupts all running jobs and the `events` and `notifications` streams.
   *
   * @param jobLimit the number of jobs that may be pending before calls to
   *                 `submit` (semantically) block
   * @param notificationsLimit the number of unconsumed notifications after which
   *                           notification attempts will begin (semantically)
   *                           blocking jobs
   * @param eventsLimit the number of unconsumed termination events after which
   *                    the oldest termination event will be evicted.
   * @param jobConcurrency the number of submitted jobs to run concurrently.
   */
  def apply[F[_]: Concurrent: Timer, I, N](
      jobLimit: Int = 100,
      notificationsLimit: Int = 10,
      eventsLimit: Int = 100,
      jobConcurrency: Int = 100)
      : Resource[F, JobManager[F, I, N]] = {

    val s = for {
      notificationsQ <- Stream.eval(Queue.bounded[F, Option[(I, N)]](notificationsLimit))
      eventQ <- Stream.eval(Queue.circularBuffer[F, Option[Event[I]]](eventsLimit))
      dispatchQ <- Stream.eval(Queue.bounded[F, Stream[F, Nothing]](jobLimit))

      initF = Concurrent[F] delay {
        new JobManager[F, I, N](
          notificationsQ,
          eventQ,
          dispatchQ)
      }

      jm <- Stream.bracketWeak(initF)(_.shutdown)
      back <- Stream.emit(jm).concurrently(dispatchQ.dequeue.parJoin(jobConcurrency))
    } yield back

    s.compile.resource.lastOrError
  }

  private final case class Context[F[_]](status: Status, cancel: F[Unit])
}
