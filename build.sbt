// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

val ScalatraVersion = "2.6.3"

organization := "com.bwsw"

name := "cs-kv-storage"

version := "1.0.0"

scalaVersion := "2.12.6"

resolvers += Classpaths.typesafeReleases

val elastic4sVersion = "6.2.8"
val akkaVersion = "2.5.12"

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.4.9.v20180320" % "container;compile",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "com.sksamuel.elastic4s" % "elastic4s-http_2.12" % elastic4sVersion,
  "com.typesafe" % "config" % "1.3.3",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.1.3",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scalatra" %% "scalatra-json" % ScalatraVersion,
  "org.json4s" %% "json4s-jackson" % "3.5.4",
  "com.github.blemale" % "scaffeine_2.12" % "2.5.0",
  "org.scaldi" % "scaldi-akka_2.12" % "0.5.8",

  // for logging filters
  "org.codehaus.janino" % "janino" % "3.0.8",

  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.github.tomakehurst" % "wiremock" % "2.18.0" % Test
)

assemblyJarName := s"${name.value}-${version.value}-jar-with-dependencies.jar"
mainClass in assembly := Some("com.bwsw.cloudstack.storage.kv.app.JettyLauncher")

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
enablePlugins(DockerPlugin)

dockerfile in docker := {
  val artifact: File = assembly.value
  val appDirectory = "/opt/cs-kv-storage"
  val appPath = s"$appDirectory/${artifact.name}"

  new Dockerfile {
    from("openjdk:8-alpine")
    expose(8080)
    volume("/var/log/cs-kv-storage")
    add(artifact, appPath)
    entryPoint("java", s"-Dconfig.file=$appDirectory/application.conf", "-jar", appPath)
  }
}
imageNames in docker := Seq(
  ImageName(
    namespace = sys.props.get("docker.registry").orElse(Some("git.bw-sw.com:5000/cloudstack-ecosystem")),
    repository = sys.props.get("docker.image").getOrElse(name.value),
    tag = sys.props.get("docker.tag").orElse(Some(version.value)))
)
buildOptions in docker := BuildOptions(cache = false)
