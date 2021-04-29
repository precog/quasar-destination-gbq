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

import quasar.api.ColumnType
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, LegacyDestination, ResultSink}

import argonaut._, Argonaut._

import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource}
import cats.implicits._

import org.http4s.{
  AuthScheme,
  Credentials,
  Method,
  Request,
  Status
}
import org.http4s.client._
import org.http4s.headers.Authorization
import org.slf4s.Logging

import scala.{
  Either,
  StringContext,
  Unit
}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

class GBQDestination[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect] private (
    client: Client[F],
    config: GBQConfig)
    extends LegacyDestination[F]
    with GBQSinks[F]
    with Logging {

  def destinationType: DestinationType =
     GBQDestinationModule.destinationType

  def flowF(args: GBQSinks.Args) = GBQFlow(config, client, args)

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    flowSinks

  def maxFileSize = config.maxFileSize getOrElse GBQDestination.DefaultMaxFileSize
}

object GBQDestination {
  val DefaultMaxFileSize = 1024L * 1024L * 1024L

  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect](config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {

    val configOrError = config.as[GBQConfig].toEither.leftMap {
      case (err, _) =>
        DestinationError.malformedConfiguration(
          (GBQDestinationModule.destinationType, jString(GBQConfig.Redacted), err))
    }

    val init = for {
      cfg <- EitherT(Resource.pure[F, Either[InitializationError[Json], GBQConfig]](configOrError))
      client <- EitherT(AsyncHttpClientBuilder[F].map(_.asRight[InitializationError[Json]]))
      _ <- EitherT(Resource.liftF(isLive(client, cfg)))
    } yield new GBQDestination[F](client, cfg): Destination[F]

    init.value
  }

  private def isLive[F[_]: Concurrent: ContextShift](
      client: Client[F],
      config: GBQConfig)
      : F[Either[InitializationError[Json], Unit]] = {
    val project = URLEncoder.encode(config.authCfg.projectId, StandardCharsets.UTF_8.toString)
    val datasetsUrlString = s"https://www.googleapis.com/bigquery/v2/projects/${project}/datasets"
    val uriEither = StringToUri.get(datasetsUrlString)
    for {
      accessToken <- GBQAccessToken.token(config.serviceAccountAuthBytes)
      auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))
      response <- uriEither.fold(_.asLeft[Unit].pure[F], uri => {
        val request = Request[F](Method.GET, uri).withHeaders(auth)
        client.run(request).use { resp =>
          resp.status match {
            case Status.Ok => ().asRight[InitializationError[Json]].pure[F]
            case _ => DestinationError.malformedConfiguration((
              GBQDestinationModule.destinationType,
              jString(resp.status.reason),
              config.sanitizedJson.toString)).asLeft[Unit].pure[F]
          }
        }
      })
    } yield response
  }
}
