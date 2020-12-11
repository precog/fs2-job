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

package fs2.job

import scala.{Product, Serializable}
import scala.concurrent.duration.FiniteDuration

import java.lang.Throwable

sealed trait Event[I] extends Product with Serializable {
  def id: I
  def start: Timestamp
  def duration: FiniteDuration
}

object Event {
  final case class Completed[I](id: I, start: Timestamp, duration: FiniteDuration) extends Event[I]
  final case class Failed[I](id: I, start: Timestamp, duration: FiniteDuration, ex: Throwable) extends Event[I]
}

final case class Timestamp(epoch: FiniteDuration)
