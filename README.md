# kvcouchbase-provider capability provider

This capability provider implements the `wasmcloud:keyvalue` capability contract with a Couchbase back-end. 

Build with `make`. Test with `make test`.

The test program in tests/kv_test.rs has example code for using
each of this provider's functions.

## Link Definition Configuration Settings

The following is a list of configuration settings available in the link definition.

| Property     | Description                                           |
|:-------------|:------------------------------------------------------|
| `URL`        | The connection string URL for the Couchbase database. |
| `bucket`     | The bucket to connect to.                             |
| `collection` | The collection to connect to.                         |
| `username`   | username.                                             |
| `password`   | password.                                             |

## Configuring a default Couchbase URL

This provider also accepts a default URL as a configuration value on startup to override the default URL. This can be useful to easily setup multiple actors to access the same default endpoint without specifying the URL in the link definition.

```json
{
  "url": "couchbase://127.0.0.1" ,
  "bucket": "default" ,
  "collection": "default" ,
  "username": "Administrator" ,
  "password": "Administrator"
}
```

### Using the included Github Actions
If you store your source code on Github, we've gone ahead and included two actions: `build.yml` and `release.yml` under `.github/workflows`. The build action will automatically build, lint, and check formatting for your actor. The release action will automatically release a new version of your actor whenever code is pushed to `main`, or when you push a tag with the form `vX.Y.Z`. 

These actions require 3 secrets
1. `WASH_ISSUER_KEY`, which can be generated with `wash keys gen issuer`, then look for the 58 character `Seed` value
1. `WASH_SUBJECT_KEY`, which can be generated with `wash keys gen module`, then look for the 58 character `Seed` value
1. `WASMCLOUD_PAT`, which can be created by following the [Github PAT instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) and ensuring the `write:packages` permission is enabled