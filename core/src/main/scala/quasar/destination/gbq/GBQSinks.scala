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
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.connector._
import quasar.connector.destination.{ResultSink, WriteMode}, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig

import cats.data._
import cats.effect.Concurrent
import cats.implicits._

import fs2.{Stream, Pipe, Chunk}

import skolems.∀

import GBQSinks._

trait GBQSinks[F[_]] {
  type Consume[E[_], A] =
    Pipe[F, E[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def flowF(args: Args): F[Flow[F]]

  def maxFileSize: Long

  def render: RenderConfig[Byte] =
    RenderConfig.Csv(includeHeader = false, numericScale = 9.some, includeBom = false)

  def flowSinks(implicit F: Concurrent[F]): NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    NonEmptyList.of(ResultSink.create(create), ResultSink.upsert(upsert), ResultSink.append(append))

  def create(path: ResourcePath, cols: NonEmptyList[FlowColumn])(implicit F: Concurrent[F])
      : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {
    val args = Args.ofCreate(path, cols)
    (render, in => for {
      flow <- Stream.eval(flowF(args))
      _ <- in
        .through(Event.fromByteStream[F](maxFileSize))
        .through(upsertPipe0[Unit](flow))
    } yield ())
  }

  def upsert(upsertArgs: UpsertSink.Args[ColumnType.Scalar])(implicit F: Concurrent[F])
      : (RenderConfig[Byte], ∀[Consume[DataEvent[Byte, *], *]]) = {
    val args = Args.ofUpsert(upsertArgs)
    val consume = ∀[Consume[DataEvent[Byte, *], *]](upsertPipe(args))
    (render, consume)
  }

  def append(appendArgs: AppendSink.Args[ColumnType.Scalar])(implicit F: Concurrent[F])
      : (RenderConfig[Byte], ∀[Consume[AppendEvent[Byte, *], *]]) = {
    val args = Args.ofAppend(appendArgs)
    val consume = ∀[Consume[AppendEvent[Byte, *], *]](upsertPipe(args))
    (render, consume)
  }

  private def upsertPipe[A](args: Args)(implicit F: Concurrent[F])
      : Pipe[F, DataEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = { events =>
    for {
      flow <- Stream.eval(flowF(args))
      offset <- events
        .through(Event.fromDataEvent[F, OffsetKey.Actual[A]](maxFileSize))
        .through(upsertPipe0[OffsetKey.Actual[A]](flow))
    } yield offset
  }

  private def upsertPipe0[A](flow: Flow[F])
      : Pipe[F, Event[F, A], A] = { events =>
    def handleEvent: Pipe[F, Event[F, A], Option[A]] = _ flatMap {
      case Event.Create(stream) =>
        flow.ingest(stream).mapChunks(_ => Chunk(none[A]))
      case Event.Delete(ids) =>
        flow.delete(ids).mapChunks(_ => Chunk(none[A]))
      case Event.Commit(offset) =>
        Stream.emit(offset.some)
    }
    events.through(handleEvent).unNone
  }
}

object GBQSinks {
  private type FlowColumn = Column[ColumnType.Scalar]

  sealed trait Args {
    def path: ResourcePath
    def columns: NonEmptyList[FlowColumn]
    def writeMode: WriteMode
    def idColumn: Option[Column[_]]
  }

  object Args {
    def ofCreate(p: ResourcePath, cs: NonEmptyList[FlowColumn]): Args = new Args {
      def path = p
      def columns = cs
      def writeMode = WriteMode.Replace
      def idColumn = None
    }

    def ofUpsert(args: UpsertSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.idColumn.some
    }

    def ofAppend(args: AppendSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.pushColumns.primary
    }
  }
}
