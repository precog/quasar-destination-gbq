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

import slamdata.Predef._

import quasar.api.{Column, ColumnType}
import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.resource.ResourceName
import quasar.connector.{MonadResourceErr, ResourceError, IdBatch}
import quasar.connector.destination.WriteMode

import argonaut._, Argonaut._

import cats.ApplicativeError
import cats.data.{ValidatedNel, NonEmptyList}
import cats.effect.{Concurrent, Resource, Sync}
import cats.effect.concurrent.Ref
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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import com.google.auth.oauth2.AccessToken

final class GBQFlow[F[_]: Concurrent](
    name: String,
    schema: NonEmptyList[GBQSchema],
    idColumn: Option[Column[_]],
    config: GBQConfig,
    client: Client[F],
    refMode: Ref[F, WriteMode])
    extends Flow[F] 
    with Logging {

  private def credsF =
    tokenF map { token =>
      Authorization(Credentials.Token(AuthScheme.Bearer, token.getTokenValue))
    }

  private val project =
    URLEncoder.encode(config.authCfg.projectId, StandardCharsets.UTF_8.toString)

  private val uriRoot =
    s"https://bigquery.googleapis.com/bigquery/v2/projects/${project}"

  private def tokenF: F[AccessToken] =
    GBQAccessToken.token(config.serviceAccountAuthBytes)

  def ingest: Pipe[F, Byte, Unit] = { (bytes: Stream[F, Byte]) =>
    for {
      eitherloc <- Stream.eval {
        for {
          mode <- refMode.get
          jobConfig = formGBQJobConfig(schema, config, name, mode)
          _ <- mkDataset.whenA(mode === WriteMode.Replace)
          eloc <- mkGbqJob(jobConfig)
        } yield eloc
      }
      _ <- eitherloc match {
        case Right(locationUri) => upload(bytes, locationUri)
        case Left(e) => Stream.raiseError[F](new RuntimeException(s"No Location URL returned from job: $e"))
      }
      _ <- Stream.eval(refMode.set(WriteMode.Append))
    } yield ()
  }

  def delete(ids: IdBatch): Stream[F, Unit] = idColumn traverse_ { (col: Column[_]) =>
    val strs: Array[String] = ids match {
      case IdBatch.Strings(values, _) => values.map(x => "\"" + x + "\"")
      case IdBatch.Longs(values, _) => values.map(_.toString)
      case IdBatch.Doubles(values, _) => values.map(_.toString)
      case IdBatch.BigDecimals(values, _) => values.map(_.toString)
    }

    def mkIn(strs: Array[String]): String = {
      if (strs.isEmpty) ""
      else s"`${col.name}` IN (${strs.mkString(", ")})"
    }

    // We group ids into 64 sized batches like
    // foo in (a0...a63) or foo in (a64...a127)
    // because there is definitely a limit for `IN` sentence and
    // it's definitely more than 64.
    def group(ix: Int, accum: List[String]): String = {
      val nextIx = ix + 64
      val taken = strs.slice(ix, nextIx)
      val in = mkIn(taken)
      val nextAccum = in :: accum
      if (nextIx > ids.size + 1)
        nextAccum.mkString(" OR ")
      else
        group(nextIx, nextAccum)
    }

    val grouped = group(0, List())

    if (grouped.isEmpty) {
      // Nothing to delete
      ().pure[Stream[F, *]]
    } else {
      val query =
        s"DELETE FROM `${config.authCfg.projectId}.${config.datasetId}.$name` WHERE $grouped"

      val qConfig = QueryJobConfig(query, 6L * 3600L * 1000L)

      filter(qConfig)
    }
  }

  private def filter(q: QueryJobConfig): Stream[F, Unit] = {
    implicit def entityEncoder: EntityEncoder[F, QueryJobConfig] =
      jsonEncoderOf[F, QueryJobConfig]

    StringToUri.get(s"$uriRoot/queries") match {
      case Left(_) =>
        Stream.raiseError[F](new RuntimeException(s"Incorrect url constructed: $uriRoot/queries"))

      case Right(uri) =>
        val reqF = credsF map { creds =>
          Request[F](Method.POST, uri)
            .withHeaders(creds)
            .withContentType(`Content-Type`(MediaType.application.json))
            .withEntity(q)
        }
        for {
          req <- Stream.eval(reqF)
          resp <- client.stream(req)
          body <- Stream.eval(resp.as[String])
          _ <- resp.status match {
            case Status.Ok =>
              ().pure[Stream[F, *]]
            case x =>
              Stream.raiseError[F](new RuntimeException(
                s"An error occured during existing ids filtering. status: $x, response: $body"))
          }
        } yield ()
    }
  }

  private def formGBQJobConfig(
      schema: NonEmptyList[GBQSchema],
      config: GBQConfig,
      tableId: String,
      mode: WriteMode)
      : GBQJobConfig = {
    val disposition = mode match {
      case WriteMode.Replace => WriteDisposition("WRITE_TRUNCATE")
      case WriteMode.Append => WriteDisposition("WRITE_APPEND")
    }

    GBQJobConfig(
      "CSV",
      0,
      true,
      schema.toList,
      "DAY",
      disposition,
      GBQDestinationTable(config.authCfg.projectId, config.datasetId, tableId),
      21600000, // 6hrs load job limit
      "LOAD")
  }


  private def mkDataset: F[Either[InitializationError[Json], Unit]] = {

    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQDatasetConfig] =
      jsonEncoderOf[F, GBQDatasetConfig]

    val dCfg = GBQDatasetConfig(config.authCfg.projectId, config.datasetId)

    StringToUri.get(s"$uriRoot/datasets").fold(_.asLeft[Unit].pure[F], uri => {

      val datasetReqF = credsF map { creds =>
        Request[F](Method.POST, uri)
          .withHeaders(creds)
          .withContentType(`Content-Type`(MediaType.application.json))
          .withEntity(dCfg)
        }

      Resource.eval(datasetReqF).flatMap(client.run).use { resp =>
        resp.status match {
          case Status.Ok => ().asRight[InitializationError[Json]].pure[F]
          case Status.Conflict =>
            DestinationError.invalidConfiguration(
              (GBQDestinationModule.destinationType,
              jString(s"Reason: ${resp.status.reason}"),
              ZNEList(resp.status.reason))).asLeft[Unit].pure[F]
          case status =>
            DestinationError.malformedConfiguration(
              (GBQDestinationModule.destinationType, jString(s"Reason: ${status.reason}"),
              config.sanitizedJson.toString)).asLeft[Unit].pure[F]
        }
      }
    })
  }

  private def mkGbqJob(jCfg: GBQJobConfig): F[Either[InitializationError[Json], Uri]] = {
    implicit def jobConfigEntityEncoder: EntityEncoder[F, GBQJobConfig] = jsonEncoderOf[F, GBQJobConfig]

    val project =
      URLEncoder.encode(jCfg.destinationTable.project, StandardCharsets.UTF_8.toString)
    val jobUrlString =
      s"https://bigquery.googleapis.com/upload/bigquery/v2/projects/${project}/jobs?uploadType=resumable"

    StringToUri.get(jobUrlString).fold(_.asLeft[Uri].pure[F], uri => {
      val jobReq = credsF map { creds =>
        Request[F](Method.POST,uri)
          .withHeaders(creds)
          .withContentType(`Content-Type`(MediaType.application.json))
          .withEntity(jCfg)
      }

      Resource.eval(jobReq).flatMap(client.run).use { resp =>
        resp.status match {
          case Status.Ok => resp.headers match {
            case Location(loc) => 
              Sync[F].delay(log.debug(s"Successfully initialised job."))
                .as(loc.uri.asRight[InitializationError[Json]])
          }
          case otherStatus =>  
            resp.attemptAs[String].fold(
                _ => Sync[F].delay(log.error(s"GBQ job creation failed with status '$otherStatus' and no body")),
                body => Sync[F].delay(log.error(s"GBQ job creation failed with status '$otherStatus': $body")))
              .as(DestinationError.invalidConfiguration(
                (GBQDestinationModule.destinationType, 
                  config.sanitizedJson,
                  ZNEList(s"Error creating GBQ job: ${resp.status.reason}"))).asLeft[Uri])

            
        }
      }
    })
  }

  private def upload(bytes: Stream[F, Byte], uploadLocation: Uri): Stream[F, Unit] = {
    val destReq = Request[F](Method.PUT, uploadLocation)
        .putHeaders(Header("Host", "www.googleapis.com"))
        .withContentType(`Content-Type`(MediaType.application.`x-www-form-urlencoded`))
        .withEntity(bytes)
    client.stream(destReq) evalMap { resp =>
      resp.status match {
        case Status.Ok => ().pure[F]
        case _ => ApplicativeError[F, Throwable].raiseError[Unit](
            new RuntimeException(s"Upload failed: ${resp.status.reason}" ))
      }
    }
  }
}

object GBQFlow {
  def apply[F[_]: Concurrent: MonadResourceErr](
      config: GBQConfig,
      client: Client[F],
      args: GBQSinks.Args)
      : F[Flow[F]] = {
    val tableNameF = args.path.uncons match {
      case Some((ResourceName(name), _)) => name.pure[F]
      case _ => MonadResourceErr[F].raiseError[String](ResourceError.notAResource(args.path))
    }
    for {
      name <- tableNameF
      ref <- Ref.of[F, WriteMode](args.writeMode)
      columns <- ensureValidColumns[F](args.columns)
    } yield new GBQFlow(name, columns, args.idColumn, config, client, ref)
  }

  private def ensureValidColumns[F[_]: Sync](
      columns: NonEmptyList[Column[ColumnType.Scalar]])
      : F[NonEmptyList[GBQSchema]] =
    columns.traverse(mkColumn(_)).toEither match {
      case Right(a) => a.pure[F]
      case Left(errs) => Sync[F].raiseError { new RuntimeException {
        s"Some column types are not supported: ${mkErrorString(errs)}"
      }}
    }

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Google Big Query")
      .intercalate(", ")

  private def mkColumn(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, GBQSchema] =
    tblColumnToGbq(c.tpe).map(s => GBQSchema(s, hygienicIdent(c.name)))

  /*
   * From: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
   *
   * The name must contain only letters (a-z, A-Z), numbers (0-9), 
   * or underscores (_), and must start with a letter or underscore. 
   *
   * This regex will replace underscores with underscores, but I think I can live with that
   */
  private val disallowedCharacterRegex = raw"((\W))".r

  private def hygienicIdent(ident: String): String = 
    disallowedCharacterRegex.replaceAllIn(ident, "_")

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
}
