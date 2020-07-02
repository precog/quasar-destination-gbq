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

import argonaut._, Argonaut._

import cats.ApplicativeError
import cats.data.{ValidatedNel, NonEmptyList}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync}
import cats.implicits._

import fs2.Stream

import java.lang.{RuntimeException, Throwable}

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
import org.http4s.client.Client
import org.http4s.Header
import org.http4s.headers.{Authorization, `Content-Type`, Location}
import org.slf4s.Logging
import org.http4s.client._

import quasar.api.{Column, ColumnType}
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.resource.ResourceName
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{LegacyDestination, ResultSink}
import quasar.connector.render.RenderConfig
import quasar.destination.gbq.GBQConfig._

import scala.Predef._
import scala.{
  Byte,
  Either,
  Some,
  StringContext,
  Unit
}
import scala.util.{Left, Right}

import scalaz.{NonEmptyList => ZNEList}


final class GBQDestination[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect](
    client: Client[F], 
    config: GBQConfig,
    sanitizedConfig: Json) extends LegacyDestination[F] with Logging {

  def destinationType: DestinationType =
     GBQDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] = 
    NonEmptyList.one(csvSink)

  val gbqRenderConfig: RenderConfig.Csv = 
    RenderConfig.Csv(includeHeader = false)

  private def csvSink: ResultSink[F, ColumnType.Scalar] =
    ResultSink.create[F, ColumnType.Scalar](gbqRenderConfig) { 
      case (path, columns, bytes) =>

      val tableNameF = path.uncons match {
        case Some((ResourceName(name), _)) => name.pure[F]
        case _ => MonadResourceErr[F].raiseError[String](
          ResourceError.notAResource(path))
      }

      for {
        tableName <- Stream.eval[F, String](tableNameF)
        schema <- Stream.fromEither[F](ensureValidColumns(columns).leftMap(new RuntimeException(_)))
        gbqJobConfig = formGBQJobConfig(schema, config, tableName)
        authCfgJson = config.authCfg.asJson
        accessToken <- Stream.eval(GBQAccessToken.token(authCfgJson.toString.getBytes("UTF-8")))
        _ <- Stream.eval((mkDataset(client, accessToken.getTokenValue, config)))
        eitherloc <- Stream.eval(mkGbqJob(client, accessToken.getTokenValue, gbqJobConfig))
        _ <- Stream.eval(
          Sync[F].delay(
            log.info(s"(re)creating ${config.authCfg.projectId}.${config.datasetId}.${tableName} with schema ${columns.show}")))
        _ <- eitherloc match {
            case Right(locationUri) => {
              upload(client, bytes, locationUri)
            }
            case Left(e) =>
              Stream.eval(Sync[F].delay(ApplicativeError[F, Throwable].raiseError(
                new RuntimeException(s"No Location URL from returned from job: $e"))))
          }
      } yield ()
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
      Some("DAY"), //TODO: make this configurable
      WriteDisposition("WRITE_TRUNCATE"), //TODO: make this configurable
      GBQDestinationTable(config.authCfg.projectId, config.datasetId, tableId), 
      "21600000", //6hrs load job limit
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
        case ColumnType.OffsetTime => "TIMESTAMP".validNel
        case ColumnType.OffsetDateTime => "TIMESTAMP".validNel
        case ColumnType.OffsetDate => "TIMESTAMP".validNel
        case i @ ColumnType.Interval => i.invalidNel
        case ColumnType.Null => "INTEGER1".validNel
      }

  private def mkDataset(client: Client[F], accessToken: String, config: GBQConfig): F[Either[InitializationError[Json], Unit]] = {
    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQDatasetConfig] = 
      jsonEncoderOf[F, GBQDatasetConfig]

    val dCfg = GBQDatasetConfig(config.authCfg.projectId, config.datasetId)
    val bearerToken = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken))
    val datasetReq = Request[F](
      method = Method.POST,
      uri = Uri.fromString(s"https://bigquery.googleapis.com/bigquery/v2/projects/${config.authCfg.projectId}/datasets")
        .getOrElse(Uri()))
        .withHeaders(bearerToken)
        .withContentType(`Content-Type`(MediaType.application.json))
        .withEntity(dCfg)

      client.run(datasetReq).use { resp =>
        resp.status match {
          case Status.Ok =>
            ().asRight[InitializationError[Json]].pure[F]
          case Status.Conflict =>
            DestinationError.invalidConfiguration(
              (destinationType,
              jString(s"Reason: gbq dataset ${resp.status.reason}"),
              ZNEList(resp.status.reason))).asLeft.pure[F]
          case status =>
            DestinationError.malformedConfiguration(
              (destinationType, jString("Reason: " + status.reason), 
              sanitizedConfig.toString)).asLeft.pure[F]
        }
      }
    }

  private def mkGbqJob(client: Client[F], accessToken: String, jCfg: GBQJobConfig): F[Either[InitializationError[Json], Uri]] = {
    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQJobConfig] = jsonEncoderOf[F, GBQJobConfig]

    val authToken = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken))
    val jobReq = Request[F](
      method = Method.POST,
      uri = Uri
        .fromString(s"https://bigquery.googleapis.com/upload/bigquery/v2/projects/${jCfg.destinationTable.project}/jobs?uploadType=resumable")
        .getOrElse(Uri()))
        .withHeaders(authToken)
        .withContentType(`Content-Type`(MediaType.application.json))
        .withEntity(jCfg)

    client.run(jobReq).use { resp =>
      resp.status match {
        case Status.Ok => resp.headers match {
          case Location(loc) =>
            loc.uri.asRight.pure[F]
        }
        case _ =>
          DestinationError.malformedConfiguration(
            (destinationType, jString("Reason: " + resp.status.reason), 
            sanitizedConfig.toString)).asLeft.pure[F]
      }
    }
  }

  private def upload(client: Client[F], bytes: Stream[F, Byte], uploadLocation: Uri): Stream[F, Unit] = {
    val destReq = Request[F](method = Method.PUT, uri = uploadLocation)
        .putHeaders(Header("Host", "www.googleapis.com"))
        .withContentType(`Content-Type`(MediaType.application.`x-www-form-urlencoded`))
        .withEntity(bytes)
    val doUpload = client.run(destReq).use { resp =>
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
  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr: ConcurrentEffect, C](client: Client[F], config: GBQConfig, sanitizedConfig: Json)
      : Resource[F, Either[InitializationError[C], LegacyDestination[F]]] = {
        val gbqDest: Either[InitializationError[C], LegacyDestination[F]] = 
          new GBQDestination[F](client, config, sanitizedConfig).asRight[InitializationError[C]]
          
        Resource.liftF(gbqDest.pure[F])
  }
}