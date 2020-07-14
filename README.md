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
    "datasetId": <dataset-name>
  }
}
```

- `destination-name` is what you would like to name the destination
- `service-account-json-auth-file-contents` is the contents of your service account authentication json file in string format
- `dataset-name` is the dataset name you would like your table to be pushed into
