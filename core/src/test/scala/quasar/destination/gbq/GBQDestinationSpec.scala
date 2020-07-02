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

//TODO: don't use this anymore
//import slamdata.Predef._





import argonaut._, Argonaut._

import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, Blocker, IO, Resource, Timer}
import cats.implicits._

import fs2.{Stream, text}

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors
import java.util.UUID

import org.http4s.argonaut.jsonEncoderOf
import org.http4s.client.Client
import org.http4s.{
  AuthScheme, 
  Credentials, 
  EntityEncoder, 
  Method, 
  Request, 
  Status,
  Uri}
import org.http4s.headers.Authorization
import org.http4s.headers.`Content-Type`
import org.http4s.client._

import quasar.api.{Column, ColumnType}
import quasar.api.destination.DestinationError.InitializationError
import quasar.connector.destination.{Destination, ResultSink}
import quasar.connector.render.RenderConfig
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_
import quasar.EffectfulQSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import scala.{
  Either,
  List,
  StringContext,
}
import scala.concurrent.duration._
import scala.Predef.String
import scala.util.Right
import scala.util.Left

import scalaz.{-\/,\/-}

import slamdata.Predef.RuntimeException

import shims._


object GBQDestinationSpec extends EffectfulQSpec[IO] {
  sequential

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val testProject = "precog-ci-275718"
  val testDataset = "dataset_" + UUID.randomUUID().toString.replace("-", "_").toString
  val tableName = "table_" + UUID.randomUUID().toString.replace("-", "_").toString
  val authCfgPath = Paths.get(getClass.getClassLoader.getResource("precog-ci-275718-e913743ebfeb.json").toURI)
  val authCfgString = new String(Files.readAllBytes(authCfgPath), UTF_8)
  val authCfgJson: Json = Parse.parse(authCfgString) match {
    case Left(value) => Json.obj("malformed" := true)
    case Right(value) => value
  }
  val gbqCfg = config(authCfgJson, testDataset)
  val blockingPool = Executors.newFixedThreadPool(5)
  val blocker = Blocker.liftExecutorService(blockingPool)
  val httpClient: Client[IO] = JavaNetClientBuilder[IO](blocker).create

  "csv link" should {
    "reject empty paths with NotAResource" >>* {
      csv(gbqCfg) { sink =>
        val path = ResourcePath.root()
        val req = sink.consume(path, NonEmptyList.one(Column("a", ColumnType.Boolean)), Stream.empty).compile.drain
        MRE.attempt(req).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== path
        })
      }
    }

    "successfully upload table" >>* {
      csv(gbqCfg) { sink =>
          val data = Stream("col1,col2\r\nstuff,true\r\n").through(text.utf8Encode)
          val path = ResourcePath.root() / ResourceName(tableName) / ResourceName("bar.csv")
          val req = sink.consume(
            path,
            NonEmptyList.fromList(List(Column("a", ColumnType.String), Column("b", ColumnType.Boolean))).get,
            data).compile.drain

          Timer[IO].sleep(5.seconds).flatMap { _ =>
            MRE.attempt(req).map(_ must beLike {
              case \/-(value) => value must_===(())
            })
          }
      }
    }
  }

  "bigquery upload" should {
    "successfully check dataset was created" >>* {
      for {
        accessToken <- GBQAccessToken.token[IO](authCfgString.getBytes("UTF-8"))
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))
        req = Request[IO](
          method = Method.GET,
          uri = Uri.fromString(s"https://bigquery.googleapis.com/bigquery/v2/projects/${testProject}/datasets")
            .getOrElse(Uri()))
            .withHeaders(auth)
        resp <- httpClient.run(req).use {
            case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
            case r => r.as[String].map(b => Left(s"Request ${req} failed with status ${r.status.code} and body ${b}")) 
        }
        body = resp match {
          case Left(value) => value
          case Right(value) => value
        }
        result <- IO {
           body.contains(testDataset) must beTrue
        }
      } yield result
    }

    "successfully check uploaded table exists" >>* {
      for {
        accessToken <- GBQAccessToken.token[IO](authCfgString.getBytes("UTF-8"))
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))
        req = Request[IO](
          method = Method.GET,
          uri = Uri.fromString(s"https://bigquery.googleapis.com/bigquery/v2/projects/${testProject}/datasets/${testDataset}/tables")
            .getOrElse(Uri()))
            .withHeaders(auth)
        resp <- Timer[IO].sleep(5.seconds).flatMap { _ =>
          httpClient.run(req).use {
            case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
            case r => r.as[String].map(b => Left(s"Request ${req} failed with status ${r.status.code} and body ${b}")) 
        }}
        body = resp match {
          case Left(value) => value
          case Right(value) => value
        }
        result <- IO {
           body.contains(tableName) must beTrue
        }
      } yield result
    }

    "successfully check table contents" >>* {
      for {
        accessToken <- GBQAccessToken.token[IO](authCfgString.getBytes("UTF-8"))
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))
        req = Request[IO](
          method = Method.GET,
          uri = Uri.fromString(s"https://bigquery.googleapis.com/bigquery/v2/projects/${testProject}/datasets/${testDataset}/tables/${tableName}/data")
            .getOrElse(Uri()))
            .withHeaders(auth)
        resp <- Timer[IO].sleep(5.seconds).flatMap { _ =>
          httpClient.run(req).use {
            case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
            case r => r.as[String].map(b => Left(s"Request ${req} failed with status ${r.status.code} and body ${b}"))
        }}
        body = resp match {
          case Left(value) => value
          case Right(value) => value
        }
        result <- IO {
          body.contains("stuff") must beTrue
        }
      } yield result
    }

    "successfully cleanup dataset and tables" >>* {
      for {
        accessToken <- GBQAccessToken.token[IO](authCfgString.getBytes("UTF-8"))
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))
        req = Request[IO](
          method = Method.DELETE,
          uri = Uri.fromString(s"https://bigquery.googleapis.com/bigquery/v2/projects/${testProject}/datasets/${testDataset}?deleteContents=true")
            .getOrElse(Uri()))
            .withHeaders(auth)
        resp <- httpClient.run(req).use {
            case Status.Successful(r) => IO { r.status }
            case r => IO { r.status }
        }
        result <- IO {
          resp must beLike {
            case Status.NoContent => ok
          }
        }
      } yield result
    }
  }

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def csv[A](gbqcfg: Json)(f: ResultSink.CreateSink[IO, ColumnType.Scalar] => IO[A]): IO[A] =
    dest(gbqcfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.toString))
      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { 
            case c @ ResultSink.CreateSink(_: RenderConfig.Csv, _) => c 
          }
          .map(s => f(s.asInstanceOf[ResultSink.CreateSink[IO, ColumnType.Scalar]]))
          .getOrElse(IO.raiseError(new RuntimeException("No CSV sink found!")))
    }

  def dest[A](cfg: Json)(f: Either[InitializationError[Json], Destination[IO]] => IO[A]): IO[A] =
    GBQDestinationModule.destination[IO](cfg).use(f)

  def config(authCfg: Json, datasetId: String): Json =
    ("authCfg" := authCfg) ->:
    ("datasetId" := datasetId) ->:
    jEmptyObject

  implicit def jobConfigEntityEncoder: EntityEncoder[IO, GBQJobConfig] = jsonEncoderOf[IO, GBQJobConfig]

  def mkGeneralRequest[A](
    bearerToken: Authorization,
    data: A,
    url: String,
    reqMethod: Method,
    contentType: `Content-Type`)(
    implicit a: EntityEncoder[IO,A])
    : Request[IO] = {
      Request[IO](
        method = reqMethod,
        uri = Uri.fromString(url).getOrElse(Uri()))
          .withHeaders(bearerToken)
          .withContentType(contentType)
          .withEntity(data)
    }

  val resource: Resource[IO, Client[IO]] = mkClient[IO]

  def mkClient[F[_]: ConcurrentEffect]: Resource[F, Client[F]] = {
      AsyncHttpClientBuilder[F](ConcurrentEffect[F], ExecutionContext.fromExecutor(null))
  }

  def mkRequest(client: Client[IO], req: Request[IO]) = {
    client.run(req).use { resp =>
      resp.status match {
        case Status(_) =>
          resp.asRight.pure[IO]
        case _ =>
          DestinationError.malformedConfiguration(
            (DestinationType("gbq", 1L) , jString("Reason: " + resp.status.reason), 
            "Response Code: " + resp.status.code)).asLeft.pure[IO]
      }
    }
  }

}