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

package quasar.destination.gbq

import scala.Predef.String

import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.InitializationError

import argonaut._, Argonaut._

import cats.implicits._

import org.http4s.Uri

import scala.Either
import scala.util.Left
import scala.util.Right

import scalaz.{NonEmptyList => ZNEList}


object StringToUri {
  def get(i: String): Either[InitializationError[Json], Uri] = 
    Uri.fromString(i) match {
      case Left(v) => DestinationError.invalidConfiguration((
        GBQDestinationModule.destinationType,
        jString("Reason: invalid url"),
        ZNEList(v.details))).asLeft[Uri]
      case Right(v) => v.asRight[InitializationError[Json]]
  }
}