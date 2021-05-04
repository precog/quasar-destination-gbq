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

import quasar.connector._

import cats.effect.Concurrent
import cats.effect.concurrent.{Ref, Semaphore}
import cats.implicits._

import fs2.{Pipe, Stream, Pull, Chunk}
import fs2.concurrent.Queue

sealed trait Event[+F[_], +A]

object Event {
  type Nothing1[A] = Nothing

  final case class Commit[A](value: A) extends Event[Nothing1, A]
  final case class Create[F[_]](value: Stream[F, Byte]) extends Event[F, Nothing]
  final case class Delete(value: IdBatch) extends Event[Nothing1, Nothing]

  sealed trait RefQ[F[_]] {
    def consume(chunk: Chunk[Byte]): F[Option[Stream[F, Byte]]]
    def emit: F[Option[Stream[F, Byte]]]
  }

  object RefQ {
    def apply[F[_]: Concurrent](maxSize: Long): F[RefQ[F]] = for {
      q <- Queue.unbounded[F, Option[Chunk[Byte]]]
      ref <- Ref.of[F, Queue[F, Option[Chunk[Byte]]]](q)
      size <- Ref.of[F, Long](0L)
      semaphore <- Semaphore[F](1)
    } yield new RefQ[F] {
      // RefQ.emit and RefQ.consume must not run simultaneously
      // because they both modify refs
      // But consuming could emit stream if it's bigger than maxSize
      def consume(chunk: Chunk[Byte]): F[Option[Stream[F, Byte]]] =
        semaphore.withPermit { consume0(chunk) }
      def emit: F[Option[Stream[F, Byte]]] =
        semaphore.withPermit { emit0 }

      def emit0: F[Option[Stream[F, Byte]]] = for {
        q <- ref.get
        _ <- q.enqueue1(None)
        nq <- Queue.unbounded[F, Option[Chunk[Byte]]]
        _ <- ref.set(nq)
        s <- size.getAndSet(0L)
      } yield if (s > 0) {
        q.dequeue.unNoneTerminate.flatMap(Stream.chunk).some
      } else {
        None
      }

      def consume0(chunk: Chunk[Byte]): F[Option[Stream[F, Byte]]] = for {
        q <- ref.get
        _ <- q.enqueue1(Some(chunk))
        newSize <- size.updateAndGet(_ + chunk.size)
        res <- if (newSize > maxSize) {
          emit0
        } else {
          None.pure[F]
        }
      } yield res
    }
  }

  def fromDataEvent[F[_]: Concurrent, A](maxSize: Long): Pipe[F, DataEvent[Byte, A], Event[F, A]] = {
    def go(refQ: RefQ[F], inp: Stream[F, DataEvent[Byte, A]]): Pull[F, Event[F, A], Unit] =
      inp.pull.uncons1 flatMap {
        case Some((de, tail)) =>
          val pullAction = de match {
            case DataEvent.Create(chunk) =>
              for {
                mbStream <- Pull.eval(refQ.consume(chunk))
                _ <- mbStream.traverse_(s => Pull.output1(Event.Create(s)))
              } yield ()

            case DataEvent.Delete(ids) =>
              Pull.output1(Event.Delete(ids))

            case DataEvent.Commit(offset) =>
              for {
                mbStream <- Pull.eval(refQ.emit)
                _ <- mbStream.traverse_(s => Pull.output1(Event.Create(s)))
                _ <- Pull.output1(Event.Commit(offset))
              } yield ()
          }
          pullAction >> go(refQ, tail)
        case None =>
          Pull.done
      }
    inp => for {
      refQ <- Stream.eval(RefQ[F](maxSize))
      results <- go(refQ, inp).stream
    } yield results
  }

  def fromByteStream[F[_]: Concurrent](maxSize: Long): Pipe[F, Byte, Event[F, Unit]] = {
    def go(refQ: RefQ[F], inp: Stream[F, Byte]): Pull[F, Event[F, Unit], Unit] =
      inp.pull.uncons flatMap {
        case Some((chunk, tail)) =>
          for {
            mbStream <- Pull.eval(refQ.consume(chunk))
            _ <- mbStream.traverse_(s => Pull.output1(Event.Create(s)))
            res <- go(refQ, tail)
          } yield res
        case None =>
          for {
            mbStream <- Pull.eval(refQ.emit)
            _ <- mbStream.traverse_(s => Pull.output1(Event.Create(s)))
            _ <- Pull.done
          } yield ()
      }

    inp => for {
      refQ <- Stream.eval(RefQ[F](maxSize))
      results <- go(refQ, inp).stream
    } yield results
  }
}
