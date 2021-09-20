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

import argonaut._, Argonaut._

final case class GBQJob(
  kind: String,
  etag: String,
  id: String,
  selfLink: String,
  jobReference: GBQJobReference,
  status: GBQJobStatus
)

final case class GBQJobReference(
  projectId: String,
  jobId: String,
  location: String
)

final case class GBQJobStatus(
  errorResult: Option[GBQErrorResult],
  errors: Option[List[GBQErrors]],
  state: String
)

final case class GBQErrorResult(
  reason: String,
  location: String,
  debugInfo: String,
  message: String
)

final case class GBQErrors(
  reason: String,
  location: String,
  debugInfo: String,
  message: String
)

object GBQJob {
  
  implicit val GBQJobDecodeJson: DecodeJson[GBQJob] =
    DecodeJson(c => for {
      kind <- (c --\ "kind").as[String]
      etag <- (c --\ "age").as[String]
      id <- (c --\ "id").as[String]
      selfLink <- (c --\ "selfLink").as[String]
      jobReference <- (c --\ "jobReference").as[GBQJobReference]
      status <- (c --\ "status").as[GBQJobStatus]
    } yield GBQJob(kind, etag, id, selfLink, jobReference, status))

  implicit val GBQJobReferenceJson: DecodeJson[GBQJobReference] =
    DecodeJson(c => for {
      projectId <- (c --\ "projectId").as[String]
      jobId <- (c --\ "jobId").as[String]
      location <- (c --\ "location").as[String]
    } yield GBQJobReference(projectId, jobId, location))

  implicit val GBQJobStatusDecodeJson: DecodeJson[GBQJobStatus] =
    DecodeJson(c => for {
      errorResult <- (c --\ "errorResult").as[Option[GBQErrorResult]]
      errors <- (c --\ "errors").as[Option[List[GBQErrors]]]
      state <- (c --\ "state").as[String]
    } yield GBQJobStatus(errorResult, errors, state))

  implicit val GBQErrorResultDecodeJson: DecodeJson[GBQErrorResult] =
    DecodeJson(c => for {
      reason <- (c --\ "reason").as[String]
      location <- (c --\ "location").as[String]
      debugInfo <- (c --\ "debugInfo").as[String]
      message <- (c --\ "message").as[String]
    } yield GBQErrorResult(reason, location, debugInfo, message))


  implicit val GBQErrorsDecodeJson: DecodeJson[GBQErrors] =
    DecodeJson(c => for {
      reason <- (c --\ "reason").as[String]
      location <- (c --\ "location").as[String]
      debugInfo <- (c --\ "debugInfo").as[String]
      message <- (c --\ "message").as[String]
    } yield GBQErrors(reason, location, debugInfo, message))
}


