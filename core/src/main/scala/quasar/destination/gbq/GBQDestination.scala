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

import scala.Predef._

import quasar.api.{Column, ColumnType}
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.resource.ResourceName
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{Destination, LegacyDestination, ResultSink}
import quasar.connector.render.RenderConfig

import argonaut._, Argonaut._

import cats.ApplicativeError
import cats.data.{EitherT, ValidatedNel, NonEmptyList}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync}
import cats.implicits._

import fs2.{Pipe, Stream}

import org.http4s.{
  AuthScheme,
  Credentials,
  EntityEncoder,
  MediaType,
  Method,
  Request,
  Status,
  Uri
}
import org.http4s.argonaut.jsonEncoderOf
import org.http4s.client._
import org.http4s.Header
import org.http4s.headers.{Authorization, `Content-Type`, Location}
import org.slf4s.Logging

import scalaz.{NonEmptyList => ZNEList}

import scala.{
  Byte,
  Either,
  Some,
  StringContext,
  Unit
}
import scala.util.{Left, Right}

import java.lang.{RuntimeException, Throwable}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scalaz.{NonEmptyList => ZNEList}
import cats.effect.Timer

import org.http4s.client.middleware.Retry
import org.http4s.client.middleware.RetryPolicy
import scala.concurrent.duration._
import org.http4s.Response
import scala.Boolean

class GBQDestination[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect: Timer] private (
    client: Client[F],
    config: GBQConfig,
    sanitizedConfig: Json) extends LegacyDestination[F] with Logging {

  def destinationType: DestinationType =
     GBQDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    NonEmptyList.one(csvSink)

  val gbqRenderConfig: RenderConfig.Csv =
    RenderConfig.Csv(
      includeHeader = true,
      numericScale = Some(9)) // NUMERIC data type supports inserting data of up to scale 9

  private def csvSink: ResultSink[F, ColumnType.Scalar] =
    ResultSink.create[F, ColumnType.Scalar, Byte] { (path, columns) =>
      val tableNameF = path.uncons match {
        case Some((ResourceName(name), _)) => name.pure[F]
        case _ => MonadResourceErr[F].raiseError[String](
          ResourceError.notAResource(path))
      }

      val pipe: Pipe[F, Byte, Unit] = bytes => for {
        tableName <- Stream.eval[F, String](tableNameF)
        schema <- Stream.fromEither[F](ensureValidColumns(columns).leftMap(new RuntimeException(_)))
        gbqJobConfig = formGBQJobConfig(schema, config, tableName)
        accessToken <- Stream.eval(GBQAccessToken.token(config.serviceAccountAuthBytes))
        _ <- Stream.eval(mkDataset(client, accessToken.getTokenValue, config))
        eitherloc <- Stream.eval(mkGbqJob(client, accessToken.getTokenValue, gbqJobConfig))
        _ <- Stream.eval(
          Sync[F].delay(
            log.info(s"(re)creating ${config.authCfg.projectId}.${config.datasetId}.${tableName} with schema ${columns.show}")))
        _ <- eitherloc match {
          case Right(locationUri) => upload(client, bytes, locationUri)
          case Left(e) => Stream.raiseError[F](new RuntimeException(s"No Location URL from returned from job: $e"))
        }
      } yield ()

      (gbqRenderConfig, pipe)
  }

  // NB: config overwrites when re-pushing tables
  // see schemaUpdateOptions section at
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
  private def formGBQJobConfig(
      schema: NonEmptyList[GBQSchema],
      config: GBQConfig,
      tableId: String)
      : GBQJobConfig =
    GBQJobConfig(
      "CSV",
      "1",
      "true",
      schema.toList,
      Some("DAY"),
      WriteDisposition("WRITE_TRUNCATE"), // default is to drop and replace tables when pushing
      GBQDestinationTable(config.authCfg.projectId, config.datasetId, tableId),
      "21600000", // 6hrs load job limit
      "LOAD")

  def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Avalanche")
      .intercalate(", ")

  private def ensureValidColumns(columns: NonEmptyList[Column[ColumnType.Scalar]]): Either[String, NonEmptyList[GBQSchema]] =
    columns.traverse(mkColumn(_)).toEither leftMap { errs =>
      s"Some column types are not supported: ${mkErrorString(errs)}"
    }

  private def mkColumn(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, GBQSchema] =
    tblColumnToGbq(c.tpe).map(s => GBQSchema(s, c.name))

  private def tblColumnToGbq(ct: ColumnType.Scalar): ValidatedNel[ColumnType.Scalar, String] =
    ct match {
        case ColumnType.String => "STRING".validNel
        case ColumnType.Boolean => "BOOL".validNel
        case ColumnType.Number => "NUMERIC".validNel
        case ColumnType.LocalDate => "DATE".validNel
        case ColumnType.LocalDateTime => "DATETIME".validNel
        case ColumnType.LocalTime => "TIME".validNel
        case ColumnType.OffsetTime => "STRING".validNel
        case ColumnType.OffsetDateTime => "STRING".validNel
        case ColumnType.OffsetDate => "STRING".validNel
        case i @ ColumnType.Interval => i.invalidNel
        case ColumnType.Null => "INTEGER1".validNel
      }

  private def mkDataset(client: Client[F], accessToken: String, config: GBQConfig): F[Either[InitializationError[Json], Unit]] = {
    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQDatasetConfig] =
      jsonEncoderOf[F, GBQDatasetConfig]

    val dCfg = GBQDatasetConfig(config.authCfg.projectId, config.datasetId)
    val bearerToken = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken))
    val project = URLEncoder.encode(config.authCfg.projectId, StandardCharsets.UTF_8.toString)
    val datasetUrlString = s"https://bigquery.googleapis.com/bigquery/v2/projects/${project}/datasets"
    val uri = StringToUri.get(datasetUrlString)

    uri.fold(_.asLeft[Unit].pure[F], uri => {

      val datasetReq = Request[F](Method.POST, uri)
        .withHeaders(bearerToken)
        .withContentType(`Content-Type`(MediaType.application.json))
        .withEntity(dCfg)

        client.run(datasetReq).use { resp =>
          resp.status match {
            case Status.Ok => ().asRight[InitializationError[Json]].pure[F]
            case Status.Conflict =>
              DestinationError.invalidConfiguration(
                (destinationType,
                jString(s"Reason: ${resp.status.reason}"),
                ZNEList(resp.status.reason))).asLeft[Unit].pure[F]
            case status =>
              DestinationError.malformedConfiguration(
                (destinationType, jString(s"Reason: ${status.reason}"),
                sanitizedConfig.toString)).asLeft[Unit].pure[F]
          }
        }
    })
  }

  private def mkGbqJob(client: Client[F], accessToken: String, jCfg: GBQJobConfig): F[Either[InitializationError[Json], Uri]] = {
    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQJobConfig] = jsonEncoderOf[F, GBQJobConfig]

    val authToken = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken))
    val project = URLEncoder.encode(jCfg.destinationTable.project, StandardCharsets.UTF_8.toString)
    val jobUrlString = s"https://bigquery.googleapis.com/upload/bigquery/v2/projects/${project}/jobs?uploadType=resumable"
    val uri = StringToUri.get(jobUrlString)

    uri.fold(_.asLeft[Uri].pure[F], uri => {
      val jobReq = Request[F](Method.POST,uri)
          .withHeaders(authToken)
          .withContentType(`Content-Type`(MediaType.application.json))
          .withEntity(jCfg)

      client.run(jobReq).use { resp =>
        resp.status match {
          case Status.Ok => resp.headers match {
            case Location(loc) => loc.uri.asRight[InitializationError[Json]].pure[F]
          }
          case _ =>  DestinationError.malformedConfiguration(
            (destinationType, jString("Reason: " + resp.status.reason),
            sanitizedConfig.toString)).asLeft[Uri].pure[F]
        }
      }
    })
  }

  private def upload(client: Client[F], bytes: Stream[F, Byte], uploadLocation: Uri): Stream[F, Unit] = {
    val policy = RetryPolicy[F](_ => Some(5.seconds))
    val retryClient = Retry[F](policy)(client)
    val destReq = Request[F](Method.PUT, uploadLocation)
        .putHeaders(Header("Host", "www.googleapis.com"))
        .withContentType(`Content-Type`(MediaType.application.`x-www-form-urlencoded`))
        .withEntity(bytes)
    val doUpload = retryClient.run(destReq).use { resp =>
      resp.status match {
        case Status.Ok => ().pure[F]
        case _ => ApplicativeError[F, Throwable].raiseError[Unit](
            new RuntimeException(s"Upload failed: ${resp.status.reason}" ))
      }
    }
    Stream.eval(doUpload)
  }
}

object GBQDestination {
  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect: Timer](config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {

    val sanitizedConfig: Json = GBQDestinationModule.sanitizeDestinationConfig(config)

    val configOrError = config.as[GBQConfig].toEither.leftMap {
      case (err, _) =>
        DestinationError.malformedConfiguration(
          (GBQDestinationModule.destinationType, jString(GBQConfig.Redacted), err))
    }


    // def isRetriableStatus(result: Either[Throwable, Response[F]]): Boolean =
    //   result match {
    //     case Right(resp) => RetryPolicy.RetriableStatuses(resp.status)
    //     case Left(_) => false
    //   }

    val init = for {
      cfg <- EitherT(Resource.pure[F, Either[InitializationError[Json], GBQConfig]](configOrError))
      client <- EitherT(
        AsyncHttpClientBuilder[F]
        // .map(Retry(
        //   RetryPolicy(
        //     RetryPolicy.exponentialBackoff(maxWait = 5.seconds, maxRetry = 5),
        //     (req: Request[F], result: Either[Throwable, Response[F]]) => {
        //       req.method.isIdempotent && isRetriableStatus(result)
        //     }
        //   )
        // ))
        .map(_.asRight[InitializationError[Json]])
        
        )
      _ <- EitherT(Resource.liftF(isLive(client, cfg, sanitizedConfig)))
    } yield new GBQDestination[F](client, cfg, sanitizedConfig): Destination[F]

    init.value
  }

  private def isLive[F[_]: Concurrent: ContextShift: Timer](
      client: Client[F],
      config: GBQConfig,
      sanitizedConfig: Json)
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
              sanitizedConfig.toString)).asLeft[Unit].pure[F]
          }
        }
      })
    } yield response
  }
}
