# quasar-destination-gbq [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-gbq" % <version>
```

## Configuration
```json
{
  "name": <destination-name>,
  "type": {
    "name": "gbq",
    "version": 1
  },
  "config": {
    "authCfg": <service-account-json-auth-file-contents>,
    "datasetId": <dataset-name>,
    "maxFileSize": Number?,
    "timeOutSeconds": Number?,
    "maxRetryAttempts": Number?
  }
}
```

- `destination-name` is what you would like to name the destination
- `service-account-json-auth-file-contents` is the contents of your service account authentication json file in string format
- `dataset-name` is the dataset name you would like your table to be pushed into
- `maxFileSize` is optional parameter, default to 2^30, determines size of streams uploaded to BigQuery as files, e.g. by default 30Gb stream will be uploaded as 30 files.

## Testing

In order to run the tests, you must first decrypt the secret. You'll need to have the environment variable `ENCRYPTION_PASSWORD` set.

```
./sbt "decryptSecret core/src/test/resources/precog-ci-275718-e913743ebfeb.json.enc"
```
