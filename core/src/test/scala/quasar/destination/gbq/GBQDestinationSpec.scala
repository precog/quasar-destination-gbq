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
import scala.Predef.identity

import quasar.api.{Column, ColumnType}
import quasar.api.push.{OffsetKey, PushColumns}
import quasar.connector._
import quasar.connector.destination.{Destination, PushmiPullyu, ResultSink, WriteMode}, ResultSink.{AppendSink, UpsertSink}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_
import quasar.EffectfulQSpec

import argonaut._, Argonaut._

import cats.data.NonEmptyList
import cats.effect.{Blocker, IO, Timer, Resource}

import fs2.{Pipe, Stream, Chunk}

import org.http4s.argonaut.jsonOf
import org.http4s.client._
import org.http4s.{
  Header,
  AuthScheme,
  Credentials,
  Method,
  Request,
  Status,
  Uri,
  EntityDecoder
}
import org.http4s.headers.Authorization
import org.specs2.matcher.MatchResult

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Right
import scala.util.Left

import scalaz.-\/

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors
import java.util.UUID

//import org.specs2.matcher.MatchResult

import shims._
//import shapeless.PolyDefns.identity
import skolems.∀

object GBQDestinationSpec extends EffectfulQSpec[IO] {
  sequential

  import GBQConfig.serviceAccountConfigCodecJson

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val TEST_PROJECT = "precog-ci-275718"
  val AUTH_FILE = "precog-ci-275718-e913743ebfeb.json"

  val configAndTableF: IO[(GBQConfig, String)] = for {
    dataset <- IO("dataset_" + UUID.randomUUID.toString.replace("-", "_").toString)
    tableName <- IO("table_" + UUID.randomUUID.toString.replace("-", "_").toString)
  } yield {
    val authCfgPath = Paths.get(getClass.getClassLoader.getResource(AUTH_FILE).toURI)
    val authCfgString = new String(Files.readAllBytes(authCfgPath), UTF_8)
    val authCfgJson: Json = Parse.parse(authCfgString) match {
      case Left(value) => Json.obj("malformed" := true)
      case Right(value) => value
    }
    val config = GBQConfig(authCfgJson.as[ServiceAccountConfig].toOption.get, dataset, None)
    (config, tableName)
  }

  val blockingPool = Executors.newFixedThreadPool(5)
  val blocker = Blocker.liftExecutorService(blockingPool)
  val httpClient: Client[IO] = JavaNetClientBuilder[IO](blocker).create

  val UriRoot = s"https://bigquery.googleapis.com/bigquery/v2/projects/${TEST_PROJECT}/datasets"

  val waitABit = Timer[IO].sleep(5.seconds)

  final case class Row(tag: String, value: Int)

  object Row {
    implicit def decode: DecodeJson[Row] = DecodeJson { c =>
      for {
        tag <- c.downField("f").downN(0).downField("v").as[String]
        value <- c.downField("f").downN(1).downField("v").as[Int]
      } yield Row(tag, value)
    }
    implicit def entityDecoder: EntityDecoder[IO, Row] =
      jsonOf
  }

  final case class Rows(values: List[Row])
  object Rows {
    implicit def decode: DecodeJson[Rows] = DecodeJson { c =>
      c.downField("rows").as[List[Row]].map(Rows(_))
    }
    implicit def entityDecoder: EntityDecoder[IO, Rows] =
      jsonOf

  }

  implicit def encoder: EntityDecoder[IO, Json] =
    jsonOf

  "csv link" should {
    "reject empty paths with NotAResource" >>* {
      val path = ResourcePath.root()
      for {
        (conf, _) <- configAndTableF
        resp <- MRE.attempt(csv(conf.asJson).use({ consume =>
          consume(path, NonEmptyList.one(Column("a", ColumnType.Boolean)))
            .apply(Stream.empty)
            .compile.drain
        }))
      } yield resp must beLike {
        case -\/(ResourceError.NotAResource(p2)) => p2 must_=== path
      }
    }
  }

  "bigquery upload" should {
    "create sink successfully uploads table" >> {
       val data =
         Stream(
           Chunk.array("col1,false\r\n".getBytes),
           Chunk.array("stuff,true\r\n".getBytes))
         .flatMap(Stream.chunk(_))

       def check(f: GBQConfig => GBQConfig): IO[MatchResult[Boolean]] = {
         for {
           (config0, tableName) <- configAndTableF
           config = f(config0)
           path = ResourcePath.root() / ResourceName(tableName) / ResourceName("bar.csv")
           req = csv(config.asJson).use { consume =>
             data
               .through(consume(
                 path,
                 NonEmptyList.of(Column("a", ColumnType.String), Column("b", ColumnType.Boolean))))
               .compile.drain
           }
           consumedR <- req.attempt
           _ <- waitABit

           accessToken <- GBQAccessToken.token[IO](config.serviceAccountAuthBytes)
           auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))

           datasetReq = Request[IO](
             method = Method.GET,
             uri = Uri.fromString(UriRoot)
               .getOrElse(Uri()))
               .withHeaders(auth)
           datasetResp <- httpClient.run(datasetReq) use {
             case Status.Successful(r) => r.as[String]
             case r => IO.raiseError(new Throwable("Dataset request failed"))
           }
           _ <- waitABit

           tableReq = Request[IO](
             method = Method.GET,
             uri = Uri.fromString(s"$UriRoot/${config.datasetId}/tables")
               .getOrElse(Uri()))
               .withHeaders(auth)
           tableResp <- httpClient.run(tableReq).use {
             case Status.Successful(r) => r.as[String]
             case r => IO.raiseError(new Throwable("Table request failed"))
           }
           _ <- waitABit


           contentResponse <- getContent[String](config.datasetId, tableName, auth)
           _ <- waitABit

           deleted <- deleteDataset(config.datasetId, auth)

         } yield {
           {
             deleted must beTrue
             consumedR must beRight(())
             datasetResp.contains(config.datasetId) must beTrue
             tableResp.contains(tableName) must beTrue
             contentResponse.contains("stuff") must beTrue
             contentResponse.contains("true") must beTrue
             contentResponse.contains("false") must beTrue
             contentResponse.contains("col1") must beTrue
           }.pendingUntilFixed
         }
       }

       "default (1GB) max file size" >>* check(identity)
       "0 max file size (one file per row)" >>* check(_.copy(maxFileSize = Some(0L)))
    }

    "append sink upload tables and append data" >>* {
      val data0: Stream[IO, AppendEvent[Byte, OffsetKey.Actual[String]]] = Stream(
        DataEvent.Create(Chunk.array("a,1\r\n".getBytes)),
        DataEvent.Create(Chunk.array("b,2\r\nc,3\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit0")))

      val data1: Stream[IO, AppendEvent[Byte, OffsetKey.Actual[String]]] = Stream(
        DataEvent.Create(Chunk.array("d,4\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit1")),
        DataEvent.Create(Chunk.array("e,5\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit2")))

      val columns = PushColumns.NoPrimary(NonEmptyList.of(
        Column("tag", ColumnType.String),
        Column("value", ColumnType.Number)))

      for {
        (config, tableName) <- configAndTableF
        path = ResourcePath.root() / ResourceName(tableName) / ResourceName("bar.csv")
        args = AppendSink.Args(path, columns, WriteMode.Replace)
        accessToken <- GBQAccessToken.token[IO](config.serviceAccountAuthBytes)
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))

        r <- append(config.asJson).use { mkConsumer =>
          for {
            res0 <- data0.through(mkConsumer(args).apply[String]).compile.toList
            _ <- waitABit

            rows0 <- getContent[Rows](config.datasetId, tableName, auth)

            res1 <- data1.through(mkConsumer(args.copy(writeMode = WriteMode.Append)).apply[String])
              .compile.toList
            _ <- waitABit

            rows1 <- getContent[Rows](config.datasetId, tableName, auth)

            _ <- deleteDataset(config.datasetId, auth)

          } yield {
            val rowsExpected0 = List(
              Row("a", 1),
              Row("b", 2),
              Row("c", 3))

            val rowsExpected1 = rowsExpected0 ++ List(
              Row("d", 4),
              Row("e", 5))

            rows0 must_=== Rows(rowsExpected0)
            rows1 must_=== Rows(rowsExpected1)
            res0 must_=== List(OffsetKey.Actual.string("commit0"))
            res1 must_=== List(OffsetKey.Actual.string("commit1"), OffsetKey.Actual.string("commit2"))
          }
        }
      } yield r
    }

    "upsert sink upload tables and upsert data" >>* {
      val data0: Stream[IO, DataEvent[Byte, OffsetKey.Actual[String]]] = Stream(
        DataEvent.Create(Chunk.array("a,1\r\n".getBytes)),
        DataEvent.Create(Chunk.array("b,2\r\nc,3\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit0")))

      val data1: Stream[IO, DataEvent[Byte, OffsetKey.Actual[String]]] = Stream(
        // Checking that empty delte doesn't drop the table
        DataEvent.Delete(IdBatch.Strings(Array(), 0)),
        DataEvent.Create(Chunk.array("d,4\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit1")),
        DataEvent.Create(Chunk.array("e,5\r\n".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit2")))

      val data2: Stream[IO, DataEvent[Byte, OffsetKey.Actual[String]]] = Stream(
        DataEvent.Delete(IdBatch.Strings(Array("b", "d"), 2)),
        DataEvent.Create(Chunk.array("b,40\r\ne,21\r\nh,42".getBytes)),
        DataEvent.Commit(OffsetKey.Actual.string("commit3")))


      for {
        (config, tableName) <- configAndTableF

        path = ResourcePath.root() / ResourceName(tableName) / ResourceName("quux.csv")

        args = UpsertSink.Args[ColumnType.Scalar](
          path,
          Column("Tag", ColumnType.String),
          List(Column("Value", ColumnType.Number)),
          WriteMode.Replace)

        accessToken <- GBQAccessToken.token[IO](config.serviceAccountAuthBytes)
        auth = Authorization(Credentials.Token(AuthScheme.Bearer, accessToken.getTokenValue))

        r <- upsert(config.asJson).use { mkConsumer =>
          for {
            res0 <- data0.through(mkConsumer(args).apply[String]).compile.toList
            _ <- waitABit

            rows0 <- getContent[Rows](config.datasetId, tableName, auth)

            res1 <- data1.through(mkConsumer(args.copy(writeMode = WriteMode.Append)).apply[String])
              .compile.toList
            _ <- waitABit

            rows1 <- getContent[Rows](config.datasetId, tableName, auth)

            res2 <- data2.through(mkConsumer(args.copy(writeMode = WriteMode.Append)).apply[String])
              .compile.toList
            _ <- waitABit

            rows2 <- getContent[Rows](config.datasetId, tableName, auth)

            _ <- deleteDataset(config.datasetId, auth)
          } yield {
            val rowsExpected0 = List(
              Row("a", 1),
              Row("b", 2),
              Row("c", 3))

            val rowsExpected1 = rowsExpected0 ++ List(
              Row("d", 4),
              Row("e", 5))

            val rowsExpected2 = List(
              Row("e", 5),
              Row("a", 1),
              Row("c", 3),
              Row("b", 40),
              Row("e", 21),
              Row("h", 42))

            rows0 must_=== Rows(rowsExpected0)
            rows1 must_=== Rows(rowsExpected1)
            rows2 must_=== Rows(rowsExpected2)
            res0 must_=== List(OffsetKey.Actual.string("commit0"))
            res1 must_=== List(OffsetKey.Actual.string("commit1"), OffsetKey.Actual.string("commit2"))
            res2 must_=== List(OffsetKey.Actual.string("commit3"))
          }
        }
      } yield r

    }
  }

  def getContent[A: EntityDecoder[IO, *]](dataset: String, tableName: String, auth: Header): IO[A] = {
      val contentRequest = Request[IO](
        method = Method.GET,
        uri = Uri.fromString(s"$UriRoot/$dataset/tables/$tableName/data")
          .getOrElse(Uri()))
          .withHeaders(auth)
      httpClient.run(contentRequest).use {
        case Status.Successful(r) => r.as[A]
        case r => IO.raiseError(new Throwable("Content request errored"))
      }
  }

  def deleteDataset(dataset: String, auth: Header): IO[Boolean] = {
    val deleteReq = Request[IO](
      method = Method.DELETE,
      uri = Uri.fromString(s"$UriRoot/${dataset}?deleteContents=true")
        .getOrElse(Uri()))
        .withHeaders(auth)
    httpClient.run(deleteReq).use {
      case Status.NoContent(r) => IO(true)
      case r => IO(false)
    }
  }

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def dest(cfg: Json): Resource[IO, Destination[IO]] = {
    val pushPull: PushmiPullyu[IO] = _ => _ => Stream.empty[IO]
    val getAuth: GetAuth[IO] = _ => IO.pure(None)
    GBQDestinationModule.destination[IO](cfg, pushPull, getAuth) evalMap {
      case Left(e) => IO.raiseError(new Throwable("Incorrect config"))
      case Right(a) => IO.pure(a)
    }
  }

  type CreateConsumer = (ResourcePath, NonEmptyList[Column[ColumnType.Scalar]]) => Pipe[IO, Byte, Unit]

  def csv(gbqcfg: Json): Resource[IO, CreateConsumer] = dest(gbqcfg) evalMap { dst =>
    dst.sinks.toList
      .collectFirst({
        case c @ ResultSink.CreateSink(_) => c
      })
      .map({ s =>
        val sink = s.asInstanceOf[ResultSink.CreateSink[IO, ColumnType.Scalar, Byte]]
        val consumer: CreateConsumer = sink.consume(_, _)._2
        IO(consumer)
      })
      .getOrElse(IO.raiseError(new Throwable("No Create CSV sink found")))
  }

  type UpsertConsumer =
    UpsertSink.Args[ColumnType.Scalar] =>
      ∀[λ[α => Pipe[IO, DataEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]

  def upsert(gbqcfg: Json): Resource[IO, UpsertConsumer] = dest(gbqcfg) evalMap { dst =>
    dst.sinks.toList
      .collectFirst({
        case c @ ResultSink.UpsertSink(_) => c
      })
      .map({ s =>
        val sink = s.asInstanceOf[ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]]
        val consumer: UpsertConsumer = sink.consume(_)._2
        IO(consumer)
      })
      .getOrElse(IO.raiseError(new Throwable("No Upsert CSV sink found")))
  }

  type AppendConsumer =
    AppendSink.Args[ColumnType.Scalar] =>
      ∀[λ[α => Pipe[IO, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]

  def append(gbqcfg: Json): Resource[IO, AppendConsumer] = dest(gbqcfg) evalMap { dst =>
    dst.sinks.toList
      .collectFirst({
        case c @ ResultSink.AppendSink(_) => c
      })
      .map({ s =>
        val consume: AppendConsumer = { (x: AppendSink.Args[ColumnType.Scalar]) =>
          s.asInstanceOf[ResultSink.AppendSink[IO, ColumnType.Scalar]]
            .consume(x)
            .pipe
            .asInstanceOf[∀[λ[α => Pipe[IO, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]]
        }
        IO(consume)
      })
      .getOrElse(IO.raiseError(new Throwable("No Append CSV sink found")))
  }

}
