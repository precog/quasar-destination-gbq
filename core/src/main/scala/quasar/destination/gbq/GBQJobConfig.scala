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

import scala.Predef.String
import argonaut._ , Argonaut._
import cats.implicits._
import scala.{List, Option}

final case class GBQDestinationTable(project: String, dataset: String, table: String)
  
final case class  WriteDisposition(value: String)
final case class GBQSchema(typ: String, name: String)

final case class GBQJobConfig(
  sourceFormat: String,
  skipLeadingRows: String,
  allowQuotedNewLines: String,
  schema: List[GBQSchema], 
  timePartition: Option[String],
  writeDisposition: WriteDisposition,
  destinationTable: GBQDestinationTable,
  jobTimeoutMs: String,
  jobType: String)

object GBQJobConfig {

  implicit val GBQJobConfigCodecJson: CodecJson[GBQJobConfig] = CodecJson(
    { cfg => Json.obj(
      "configuration" := Json.obj(
        "load" := Json.obj(
          "sourceFormat" := cfg.sourceFormat,
          "skipLeadingRows" := cfg.skipLeadingRows,
          "allowQuotedNewLines" := cfg.allowQuotedNewLines,
          "schema" := Json.obj(
            "fields" := cfg.schema.toList),
          "timePartition" := cfg.timePartition,
          "writeDisposition" := cfg.writeDisposition,
          "destinationTable" := cfg.destinationTable),
        "jobTimeoutMs" := cfg.jobTimeoutMs,
        "jobType" := cfg.jobType))
    }, { c => {
      val load = c --\ "configuration" --\ "load"
      for {
        sourceFormat <- (load --\ ("sourceFormat")).as[String]
        skipLeadingRows <- (load --\ "skipLeadingRows").as[String]
        allowQuotedNewLines <- (load --\ "allowQuotedNewLines").as[String]
        schema <- (load --\ "schema" --\ "fields").as[List[GBQSchema]]
        timePartition <- (load --\ "timePartition").as[Option[String]]
        writeDisposition <- (load --\ "writeDisposition").as(writeDispositionCodecJson)
        destinationTable <- (load --\ "destinationTable").as[GBQDestinationTable]
        jobTimeoutMs <- (c --\ "configuration" --\ "jobTimeoutMs").as[String]
        jobType <- (c --\ "configuration" --\ "jobType").as[String]
      } yield GBQJobConfig(
        sourceFormat,
        skipLeadingRows,
        allowQuotedNewLines,
        schema,
        timePartition,
        writeDisposition,
        destinationTable,
        jobTimeoutMs,
        jobType)
    }})

  implicit val schemaCodecJson: CodecJson[GBQSchema] =
    casecodec2[String, String, GBQSchema](
      (typ, name) => GBQSchema(typ, name),
      gbqs => (gbqs.typ, gbqs.name).some)("type", "name")

  implicit val writeDispositionCodecJson: CodecJson[WriteDisposition] = CodecJson( 
    { wd => wd match {
        case WriteDisposition(value) => jString(value)
      }
    }, { c => c.as[String].flatMap {
        case wp @ "WRITE_APPEND" => DecodeResult.ok(WriteDisposition(wp))
        case wt @ "WRITE_TRUNCATE" => DecodeResult.ok(WriteDisposition(wt))
        case we @ "WRITE_EMPTY" => DecodeResult.ok(WriteDisposition(we))
        case other => DecodeResult.fail("Unrecognized Write Disposition: " + other, c.history)
    }})

  implicit val gbqDestinationTableCodecJson: CodecJson[GBQDestinationTable] =
    casecodec3[String, String, String, GBQDestinationTable](
      (project, dataset, table) => GBQDestinationTable(project, dataset, table),
      gdt => (gdt.project, gdt.dataset, gdt.table).some)("projectId", "datasetId", "tableId")
}