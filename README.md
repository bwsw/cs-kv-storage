Key-value storage
=================

This project provides key-value storages as an extension for Apache CloudStack.

Following storage types are supported:

* ACCOUNT

Persistent storages for Apache CloudStack accounts managed via Apache CloudStack API. Each account can have
many storages. This storage type can be configured to save a history of operations.

* VM

Storages for Apache CloudStack virtual machines created and deleted automatically while creating or expunging virtual
 machines.

* TEMP

Temporal storages with specified TTL created via Apache CloudStack API. TTL can be updated after creation. This
operation as well as storage deletion can be done via Apache CloudStack API and key-value storage API.

* [Deployment](#deployment)
* [Configuration](#configuration)
* [Build & Run](#build-run)
* [API](#api)


# Deployment #

Following components should be deployed:

* Elasticsearch 6.2

The official documentation can be found at https://www.elastic.co/guide/en/elasticsearch/reference/6.2/index.html

Once it is deployed next step is to create necessary indexes and templates. To achieve this [initialization script]
(/elasticsearch/init.sh) should be executed (use -h option to print how to use it).

* cs-kv-storage

See [configuration](#configuration) and [build & run](#build-run) sections.

# Configuration

The example of the configuration file can be found [here](/config/application.example.conf).

| Property | Description |
| --------- | ----------- |
| elasticsearch.uri | Elasticsearch addresses in the format elasticsearch://host:port,host:port, http://host:port,host:port or https://host:port,host:port. |
| elasticsearch.auth.username | Elasticsearch username for authentication. |
| elasticsearch.auth.password | Elasticsearch password for authentication. |
| elasticsearch.scroll.page-size | Batch size to retrieve all results for key listing. |
| elasticsearch.scroll.keep-alive | Timeout between batch requests to retrieve all results for key listing. |
| elasticsearch.limit.value.max-size | Max length of the value. |
| elasticsearch.limit.key.max-size | Max length of the key. |
| app.cache.max-size | Max size of the storage cache. |
| app.cache.expiration-time | TTL for the storage cache items. |
| app.history.flush-size | Size of batch requests to save a history of the storage operations. |
| app.history.flush-timeout | Timeout between batch/retry requests to save a history of the storage operations. |
| app.history.retry-limit | Amount of attempts to try to log the storage operation. |
| app.default-page-size | A default number of results returned in the page for search requests. |
| app.request-timeout | Maximum time to process the request. |

# Build & Run #

## Application as jar file
```sh
$ cd cs-kv-storage
$ sbt assembly
$ java -Dconfig.file=<config.path> -jar target/scala-2.12/cs-kv-storage-<version>-jar-with-dependencies.jar
```
where `<config.path>` and `<version>` should be replaced with actual values.

## Application as docker container

```sh
$ cd cs-kv-storage
$ sbt docker
$ docker -p <port>:8080 -v <config.path>:/opt/cs-kv-storage/application.conf git.bw-sw.com:5000/cloudstack-ecosystem/cs-kv-storage:<version>
```
where `<port>`, `<config.path>` and `<version>` should be replaced with actual values.

# API

* [Storage operations](#storage-operations)
* [Storage history](#storage-history)
* [Storage management](#storage-management)
* [Storage health](#storage-health)

## Storage operations

* [Get the value by the key](#get-the-value-by-the-key)
* [Get values by keys](#get-values-by-keys)
* [Set the value for the key](#set-the-value-for-the-key)
* [Set values for keys](#set-values-for-keys)
* [Remove the mapping by the key](#remove-the-mapping-by-the-key)
* [Remove mappings by keys](#remove-mappings-by-keys)
* [List keys](#list-keys)
* [Clear](#clear)

### Get the value by the key

#### Request

```
GET /get/<storage UUID>/<key>
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. The value is returned in the body. The content type is text/plain. |
| 404 | The storage does not exist or does not contain a mapping for the key. |
| 500 | The request can not be processed because of an internal error. |


### Get values by keys

#### Request

```
POST /get/<storage UUID>
Content-Type: application/json

[
    "key1",
    "key2",
    "key3"
]
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. Results are returned in the body as a map with null values for keys that do not exist. The content type is application/json. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

##### Body example

In the following example the mapping for the second key does not exist.

```json
{
    "key1": "value1",
    "key2": null,
    "key3": "value3"
}
```

### Set the value for the key

#### Request

```
PUT /set/<storage UUID>/somekey
Content-Type: text/plain

somevalue
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. |
| 400 | The content-type, key or value are invalid. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

### Set values for keys

#### Request

```
PUT /set/<storage UUID>
Content-Type: application/json

{
    "key1": "value1",
    "key2": "value2",
    "key3": "value3"
}
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. Results are returned in the body as a map with boolean values as an operation status. The content type is application/json.  |
| 400 | The content-type, body, key or value are invalid. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

##### Body example

In the following example values for the first and third keys are set successfully.
```json
{
    "key1": true,
    "key2": false,
    "key3": true
}
```

### Remove the mapping by the key

#### Request

```
DELETE /delete/<storage UUID>/somekey
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

### Remove mappings by keys

#### Request

```
POST /delete/<storage UUID>
Content-Type: application/json

[
    "key1",
    "key2",
    "key3"
]
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. Results are returned in the body as a map with boolean values as an operation status. The content type is application/json.  |
| 400 | The content-type or body are invalid. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

##### Body example

In the following example mappings for the first and third keys are deleted successfully.
```json
{
    "key1": true,
    "key2": false,
    "key3": true
}
```

### List keys

#### Request

```
GET /list/<storage UUID>
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. Results are returned in the body as an array. The content type is application/json.  |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

##### Body example

```json
[
    "key1",
    "key2",
    "key3"
]
```

### Clear

#### Request

```
POST /clear/<storage UUID>
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully.  |
| 409 | Conflicts occurred during executing the operation, the storage may have been cleared partially. |
| 500 | The request can not be processed because of an internal error. |

## Storage history

* [Search and list history records](#search-and-list-history-records)
* [List history records](#list-history-records)

### Search and list history records

#### Request

```
GET /history/<storage UUID>
```

##### Parameters

All parameters are optional. If both page and scroll parameters are specified scroll is used.

| Parameter  | Description |
| ----- | ----------- |
| keys  | Comma separated list of keys |
| operations | Comma separated list of operations. Possible values are set, delete or clear. |
| start | The start date/time as Unix timestamp |
| end | The end date/time as Unix timestamp |
| sort | Comma separated list of response fields optionally prefixed with - (minus) for descending order. |
| page | A page number of results (1 by default) |
| size | A number of results returned in the page/batch (default value is specified in the configuration file) |
| scroll | A timeout in ms for subsequent [list requests](#list-history-records) |

#### Response

| HTTP Status code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. Results are in the body in the format specified below. The content type is application/json.|
| 400 | The storage does not support history or the request is invalid. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

##### Body example

For requests with page parameter

```json
{
   "total":1000,
   "size":10,
   "page":1,
   "items":[
      {
         "key":"key",
         "value":"value",
         "operation":"set/delete/clear",
         "timestamp":1528442057000
      }
   ]
}
```

For requests with scroll parameter

```json
{
   "total":1000,
   "size":10,
   "scrollId":"scroll id",
   "items":[
      {
         "key":"key",
         "value":"value",
         "operation":"set/delete/clear",
         "timestamp":1528442057000
      }
   ]
}
```

### List history records

#### Request

```
POST /history
Content-Type: application/json

{
   "scrollId":"scroll id",
   "timeout": 60000
}
```

#### Response

| HTTP Status code | Body |
| ---------------- | ---- |
| 200 | The request is processed successfully. Results are in the body in the format for [search requests with scroll](#search-and-list-history-records). The content type is application/json. |
| 400 | The scroll id is invalid/expired or the request is invalid. |
| 500 | The request can not be processed because of an internal error. |

## Storage management

* [Update storage TTL](#update-storage-ttl)
* [Delete the storage](#delete-the-storage)

### Update storage TTL

#### Request

```
PUT /storage/<storage UUID>?ttl=<ttl>
```

| Parameter | Description |
|-----------|-------------|
| ttl | TTL in milliseconds |

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. |
| 400 | The storage type is not TEMP. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

### Delete the storage

#### Request

```
DELETE /storage/<storage UUID>
```

#### Response

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | The request is processed successfully. |
| 400 | The storage type is not TEMP. |
| 404 | The storage does not exist. |
| 500 | The request can not be processed because of an internal error. |

## Storage health

### Check health

#### Request

```
GET /health
```

##### Parameters

| Parameter |                                              Description                                                  |
| --------- | --------------------------------------------------------------------------------------------------------- |
| detailed  | An optional boolean parameter whether it is need to check components that it depends on. Default value - false. |

#### Response

##### Status code

| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 | Healthy |
| 500 | Unhealthy |

##### Body

If detailed = false then response body is empty, otherwise results are in the response body in the format specified
below and the content type is application/json:

```json
{
   "status":"HEALTHY/UNHEALTHY",
   "checks":[
      {
         "name":"<name>",
         "status":"HEALTHY/UNHEALTHY",
         "message":"<message>"
      }
   ]
}
```

