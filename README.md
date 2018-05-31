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

