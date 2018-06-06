# Key-Value Storage #

## Build & Run ##

### Application as jar file
```sh
$ cd cs-kv-storage
$ sbt assembly
$ java -Dconfig.file=&lt;config.path&gt; -jar target/scala-2.12/cs-kv-storage-&lt;version&gt;-jar-with-dependencies.jar 
```
where `<config.path>` and `<version>` should be replaced with actual values.

### Application as docker container

```sh
$ cd cs-kv-storage
$ sbt docker
$ docker -p &lt;port&gt;:8080 -v &lt;config.path&gt;/opt/cs-kv-storage/application.conf git.bw-sw.com:5000/cloudstack-ecosystem/cs-kv-storage:&lt;version&gt;  
```
where `<port>`, `<config.path>` and `<version>` should be replaced with actual values.

# API
## Actions
| Action | Description |
| ------ | ----------- |
| [GET /get/\<storage UUID\>/\<key\>](#get-getstorage-uuidkey) | Returns value by given key |
| [POST /get/\<storage UUID\>](#post-getstorage-uuid) | Returns values by keys given in the body of the request |
| [PUT /set/\<storage UUID\>/\<key\>](#put-setstorage-uuidkey) | Sets value given in the body by the key |
| [PUT /set/\<storage UUID\>](#put-setstorage-uuid) | Sets values by respective keys from pairs given in the body |
| [DELETE /delete/\<storage UUID\>/\<key\>](#delete-deletestorage-uuidkey) | Removes record by given key |
| [POST /delete/\<storage UUID\>](#post-deletestorage-uuid) | Removes records by array of given keys |
| [GET /list/\<storage UUID\>](#get-liststorage-uuid) | Lists keys existing in the storage |
| [POST /clear/\<storage UUID\>](#post-clearstorage-uuid) | Clears storage |
### GET /get/\<storage UUID\>/\<key\> 
#### Request example 
```
GET /get/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d/somekey
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body contains text/plain value string |
| 404 Not Found | Given storage aren't contain such key |
| 500 Internal Server Error | Server have a problem processing the request |
---
### POST /get/\<storage UUID\>
#### Request example 
```
POST /get/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d
Content-Type: application/json

[
    "key1",
    "key2",
    "key3"
]
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body contains application/json string representing map key to value or null if value cannot be found in storage|
| 500 Internal Server Error | Server have a problem processing the request |
##### Body example
In the following example values can be found only by first and third keys:
```
{
    "key1": "value1",
    "key2": null,
    "key3": "value3"
}
```
---
### PUT /set/\<storage UUID\>/\<key\>
#### Request
Request must have Content-Type set to text/plain and contain value as a simple string in the body  
For example:
```
PUT /set/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d/somekey
Content-Type: text/plain

somevalue
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body are empty |
| 400 Bad Request | Request have wrong content type or key or value are invalid |
| 500 Internal Server Error | Server have a problem processing the request |
---
### PUT /set/\<storage UUID\>
#### Request
Requset must have Content-Type set to application/json and contain map of keys and values encoded in JSON  
For example
```
PUT /set/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d
Content-Type: application/json

{
    "key1": "value1",
    "key2": "value2",
    "key3": "value3"
}
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body contains application/json with map of keys and boolean operation status |
| 500 Internal Server Error | Server have a problem processing the request |
##### Body example
In the following example values can be set only by first and third keys:
```
{
    "key1": true,
    "key2": false,
    "key3": true
}
```
---
### DELETE /delete/\<storage UUID\>/\<key\>
#### Request example
```
DELETE /delete/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d/somekey
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body is empty |
| 500 Internal Server Error | Server have a problem processing the request |
---
### POST /delete/\<storage UUID\>
#### Request
Requset must have Content-Type set to application/json and contain map of keys and values encoded in JSON  
For example
```
POST /delete/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d
Content-Type: application/json

[
    "key1",
    "key2",
    "key3"
]
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body contains application/json with map of keys and boolean operation status |
| 400 Bad Request | Request have wrong content type or body cannot be parsed |
| 500 Internal Server Error | Server have a problem processing the request |
##### Body example
In the following example values can be deleten only by first and third keys:
```
{
    "key1": true,
    "key2": false,
    "key3": true
}
```
---
### GET /list/\<storage UUID\>
#### Request example 
```
GET /list/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body contains application/json with list of keys |
| 500 Internal Server Error | Server have a problem processing the request |
##### Body example
```
[
    "key1",
    "key2":
    "key3"
]
```
---
### POST /clear/\<storage UUID\>
#### Request example 
```
POST /clear/8986a11c-3f4c-4e22-8e04-5fe35dfd3a1d
```
#### Responses
| HTTP Status Code | Description |
| ---------------- | ----------- |
| 200 OK | Body is empty |
| 409 Conflict | Conflicts occurred during deletion, storage may have been cleared partially |
| 500 Internal Server Error | Server have a problem processing the request |
# Configuration
Configuration file example can be found [here](/src/test/resources/application.conf)  
### List of parameters
| Parameter | Description |
| --------- | ----------- |
| elasticsearch.uri | Contains scheme (http or https), host and port, e.g. "http://localhost:9200" |
| elasticsearch.auth.username and elasticsearch.auth.password | Credentials used to authenticate application using X-Pack |
| elasticsearch.search.pagesize | Size limit of a single page loaded during List operation |
| elasticsearch.search.keepalive | Limit of time single page should be loaded during List action |
| elasticsearch.limit.max-value-size | Limit of how many characters value can have |
| elasticsearch.limit.max-key-size | Limit of how many characters key can have |
| app.cache.max-size | Limit the number of storages cache can have information about in a single time |
| app.cache.expiration-time | Expiration time of information stored in the cache about single storage |
| app.history.flush-size | Amount of historical records in a queue to immediately start saving it into storage |
| app.history.flush-timeout | Maximum period of time between flushes |
| app.history.retry-limit | Amount of times system tries to put record into storage in case of consecutive errors |
| app.request-timeout | Maximum time system tries to handle request |


