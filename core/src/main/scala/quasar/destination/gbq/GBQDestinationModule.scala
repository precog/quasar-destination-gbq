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

import quasar.api.destination.DestinationType
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.destination.{Destination, DestinationModule}
import quasar.connector.MonadResourceErr

import argonaut._, Argonaut._

import cats.effect.{
  ConcurrentEffect,
  ContextShift,
  Resource,
  Timer
}

import org.slf4s.Logging

import scala.{
  Some,
  Either
}

object GBQDestinationModule extends DestinationModule with Logging {

  def destinationType = DestinationType("gbq", 1L)

  def sanitizeDestinationConfig(config: Json) = {
    config.as[GBQConfig].toOption match {
      case Some(c) => Json("authCfg" := GBQConfig.Redacted, "datasetId" := c.datasetId)
      case _ => Json.jEmptyObject
    }
  }

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] =
    GBQDestination[F](config)
}
