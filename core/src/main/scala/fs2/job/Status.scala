/*
 * Copyright 2014–2020 SlamData Inc.
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

import cats.Eq

import scala.{Product, Serializable}
// import scala.concurrent.duration.FiniteDuration

sealed trait Status extends Product with Serializable

object Status {
  implicit val equal: Eq[Status] = Eq.fromUniversalEquals[Status]

  case object Pending extends Status
  case object Canceled extends Status   // kill marker to prevent already-scheduled task

  // final case class Scheduled(fromNow: FiniteDuration) extends Status
  case object Running extends Status
}
