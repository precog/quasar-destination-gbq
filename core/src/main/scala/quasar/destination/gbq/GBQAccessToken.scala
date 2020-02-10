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

import cats.effect._
import cats.effect.{Blocker, Sync}

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials

import java.io.ByteArrayInputStream
import java.util.concurrent.Executors

import quasar.concurrent.NamedDaemonThreadFactory

import scala.{
  Array,
  Byte
}
import scala.concurrent.ExecutionContext

object GBQAccessToken {
  private def genAccessToken[F[_]: Sync](auth: Array[Byte]): F[AccessToken] = Sync[F] delay {
    val authInputStream = new ByteArrayInputStream(auth)
    val credentials = GoogleCredentials
      .fromStream(authInputStream)
      .createScoped("https://www.googleapis.com/auth/bigquery")
    credentials.refreshIfExpired
    credentials.refreshAccessToken
  }

  private val blocker: Blocker =
    Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(
        Executors.newCachedThreadPool(NamedDaemonThreadFactory("gbq-destination"))))

  def token[F[_]: Sync: ContextShift](auth: Array[Byte]): F[AccessToken] =
    blocker.blockOn[F, AccessToken](genAccessToken[F](auth))
}