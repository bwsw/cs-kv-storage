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

* [Build & Run](#build-run)
* [API](#api)
* [Configuration](#configuration)  


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
* [Storage management](#storage-management)

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
    "key2":
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

| Name  | Mandatory | Description |
|-------|-----------|-------------|
| keys  | no | Comma separated list of keys. |
| operations | no | Comma separated list of operations. |
| start | no | The start date/time as Unix timestamp |
| end | no | The end  date/time as Unix timestamp |
| sort | no | Comma separated list of fields prefixed with - for descending order |
| page | no (1 by default) | A page number of results |
| size | no (default value set in configuration file) | A number of results returned in the page |
| scroll | no | A timeout for Scroll API in ms |

#### Response

| Use case  | Status code | Body |
|-------|-----------|-------------|
| The storage does not exist  | 404 | &lt;empty&gt; |
| Elasticsearch is not available | 500 | &lt;empty&gt; |
| The storage does not support history | 400 | &lt;empty&gt; |
| The storage exists and supports history | 200 | Results in the format specified below |

##### Response body examples
For requests with page parameter specified
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

For requests with scroll parameter specified
```json
{
   "total":1000,
   "size":10,
   "scroll":"scroll id",
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
   "scroll":"scroll id",
   "size": 100,
   "timeout": 60000
}
```
#### Response

| Use case  | Status code | Body |
|-------|-----------|-------------|
| Elasticsearch is not available | 500 | &lt;empty&gt; |
| Invalid/expired scroll id | 400 | &lt;empty&gt; |
| Scroll request is successful | 200 | Results in the format as specified above for requests with page and scroll |

##### Response body example

```json
{
   "total":1000,
   "size":10,
   "scroll":"scroll id",
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

# Configuration

The example of the configuration file can be found [here](/src/test/resources/application.conf).

| Property | Description |
| --------- | ----------- |
| elasticsearch.uri | Elasticsearch addesses in the format elasticsearch://host:port,host:port, http://host:port,host:port or https://host:port,host:port. |
| elasticsearch.auth.username | Elasticsearch username for authentication. |
| elasticsearch.auth.password | Elasticsearch password for authentication. | 
| elasticsearch.search.pagesize | Batch size to retrieve all results (scroll) for key listing. |
| elasticsearch.search.keepalive | Timeout between batch requests to retrieve all results (scroll) for key listing. |
| elasticsearch.limit.max-value-size | Max length of the value. |
| elasticsearch.limit.max-key-size | Max length of the key. |
| app.cache.max-size | Max size of the storage cache. |
| app.cache.expiration-time | TTL for the storage cache items. |
| app.history.flush-size | Size of batch requests to save a history of the storage operations. |
| app.history.flush-timeout | Timeout between batch/retry requests to save a history of the storage operations. |
| app.history.retry-limit | Amount of attempts to try to log the storage operation. |
| app.request-timeout | Maximum time to process the request. |


