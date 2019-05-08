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

import cats.effect.Concurrent

import scala.{Array, Option, None, Predef, Unit}, Predef.???
import scala.concurrent.duration._

import java.lang.SuppressWarnings

final class JobManager[F[_], I, N, R] private () {

  def notifications: Stream[F, N] = ???

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def submit(job: Job[F, I, N, R], delay: Option[FiniteDuration] = None): F[Unit] = ???

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def schedule(
      job: Job[F, I, N, R],
      period: FiniteDuration,
      delay: Option[FiniteDuration] = None)
      : F[Unit] = ???

  def tap(job: Job[F, I, _, R]): Stream[F, R] = ???

  def cancel(id: I): F[Unit] = ???

  def status(id: I): F[Status] = ???

  private def run: Stream[F, Unit] = ???
  private def shutdown: F[Unit] = ???
}

object JobManager {

  def apply[F[_]: Concurrent, I, N, R]: Stream[F, JobManager[F, I, N, R]] = {
    val initF = Concurrent[F].delay(new JobManager[F, I, N, R])
    Stream.bracket(initF)(_.shutdown) flatMap { jm =>
      Stream.emit(jm).concurrently(jm.run)
    }
  }
}
