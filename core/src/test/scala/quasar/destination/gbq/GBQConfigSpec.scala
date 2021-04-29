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

import argonaut.{Argonaut, DecodeJson, Json}, Argonaut._

import org.specs2.mutable.Specification

import java.net.URI

object GBQConfigSpec extends Specification {
  val decode = DecodeJson.of[GBQConfig].decodeJson(_)

  val TOKEN_URI = "https://oauth2.googleapis.com/token"
  val PROVIDER_URI = "https://www.googleapis.com/oauth2/v1/certs"
  val PRIVATE_KEY = "4t3o7pPt8IpC235DZmzqlwAx2G"
  val CLIENT_ID = "2075913892775682019834"
  val AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
  val PROJECT_ID = "myproject"
  val CLIENT_URI = s"https://www.googleapis.com/robot/v1/metadata/x509/service-account%40${PROJECT_ID}.iam.gserviceaccount.com"
  val CLIENT_EMAIL = s"service-account@${PROJECT_ID}.iam.gserviceaccount.com"
  val PRIVATE_KEY_ID = "hhgNnLfdsjb4LWPHAL5rfho"
  val ACCOUNT_TYPE = "service_account"
  val DATASET = "test_dataset"

  val jsonGbqCfg = Json.obj(
    "authCfg" := Json.obj(
        "token_uri" := TOKEN_URI,
        "auth_provider_x509_cert_url" := PROVIDER_URI,
        "private_key" := PRIVATE_KEY,
        "client_id" := CLIENT_ID,
        "client_x509_cert_url" := CLIENT_URI,
        "auth_uri" := AUTH_URI,
        "project_id" := PROJECT_ID,
        "private_key_id" := PRIVATE_KEY_ID,
        "client_email" := CLIENT_EMAIL,
        "type" := ACCOUNT_TYPE),
    "datasetId" := jString(DATASET))

  val testGbqCfg = GBQConfig(
    authCfg = ServiceAccountConfig(
      tokenUri = URI.create(TOKEN_URI),
      authProviderCertUrl = URI.create(PROVIDER_URI),
      privateKey = PRIVATE_KEY,
      clientId = CLIENT_ID,
      clientCertUrl = URI.create(CLIENT_URI),
      authUri = URI.create(AUTH_URI),
      projectId = PROJECT_ID,
      privateKeyId = PRIVATE_KEY_ID,
      clientEmail = CLIENT_EMAIL,
      accountType = ACCOUNT_TYPE),
    datasetId = DATASET,
    maxFileSize = None)

  "decode json config to GBQConfig" >> {
    decode(jsonGbqCfg).toOption must beSome(testGbqCfg)
  }

  "encode GBQConfig to json" >> {
    testGbqCfg.asJson must_=== jsonGbqCfg
  }

  "ensure GBQConfig will be readacted" >> {
    GBQDestinationModule.sanitizeDestinationConfig(jsonGbqCfg) must_===
      Json.obj("authCfg" := jString("<REDACTED>"), "datasetId" := jString(DATASET))
  }
}
