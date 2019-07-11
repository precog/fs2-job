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

package fs2.job

import scala.{Product, Serializable}

import java.lang.Throwable
import java.time.OffsetDateTime

import scala.concurrent.duration.Duration

import cats.Eq

sealed trait Event[I] extends Product with Serializable

object Event {
  final case class Completed[I](id: I, timestamp: OffsetDateTime) extends Event[I]
  final case class Failed[I](id: I, timestamp: OffsetDateTime, ex: Throwable) extends Event[I]
  final case class Canceled[I](id: I, timestamp: OffsetDateTime) extends Event[I]

  implicit def equal[I: Eq]: Eq[Event[I]] = Eq.instance[Event[I]]  {
    case (Completed(id1, _), Completed(id2, _)) => Eq[I].eqv(id1, id2)
    case (Failed(id1, _, _), Failed(id2, _, _)) => Eq[I].eqv(id1, id2)
    case _ => false
  }
}
