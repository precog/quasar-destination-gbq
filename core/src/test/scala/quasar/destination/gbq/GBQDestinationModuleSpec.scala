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

import quasar.EffectfulQSpec
import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationType
import quasar.connector.destination.Destination
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_

import argonaut._, Argonaut._

import cats.effect.{IO, Timer, ContextShift}

import scala.{Either, Left, Right}
import scala.concurrent.ExecutionContext.Implicits.global

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets.UTF_8

object GBQDestinationModuleSpec extends EffectfulQSpec[IO] {
  val AUTH_FILE = "precog-ci-275718-e913743ebfeb.json"

  val authCfgPath = Paths.get(getClass.getClassLoader.getResource(AUTH_FILE).toURI)
  val authCfgString = new String(Files.readAllBytes(authCfgPath), UTF_8)
  val authCfgJson: Json = Parse.parse(authCfgString) match {
    case Left(value) => Json.obj("malformed" := true)
    case Right(value) => value
  }

  "initialization" should {
    "fail with malformed config when config is not decodable" >>* {
      val cfg = Json("malformed" := true)
      dest(cfg)(r => IO { r match {
        case Left(DestinationError.MalformedConfiguration(_, c, _)) =>
          c must_=== jString(GBQDestinationModule.Redacted)
        case _ => ko("Expected a malformed configuration")
      }})
    }

    "successfully check project exists" >>* {
      val cfg = config(authCfgJson, "mydataset")
      dest(cfg)(r => IO { r match {
        case Right(a) => a.destinationType must_=== DestinationType("gbq", 1L)
        case _ => ko("Failed to correctly create Destination")
      }})
    }
  }

  implicit val CS: ContextShift[IO] = IO.contextShift(global)
  implicit val TM: Timer[IO] = IO.timer(global)
  implicit val MRE: MonadError_[IO, ResourceError] = MonadError_.facet[IO](ResourceError.throwableP)

  def config(authCfg: Json, datasetId: String): Json =
    ("authCfg" := authCfg) ->:
    ("datasetId" := datasetId) ->:
    jEmptyObject

  def dest[A](cfg: Json)(f: Either[InitializationError[Json], Destination[IO]] => IO[A]): IO[A] =
    GBQDestinationModule.destination[IO](cfg).use(f)
}