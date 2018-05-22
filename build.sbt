val ScalatraVersion = "2.6.3"

organization := "com.bwsw"

name := "KV Storage"

version := "0.1"

scalaVersion := "2.12.6"

resolvers += Classpaths.typesafeReleases

// major.minor are in sync with the elasticsearch releases
val elastic4sVersion = "6.2.8"

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.4.9.v20180320" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "com.typesafe.akka" %% "akka-actor" % "2.5.12",
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
  "com.sksamuel.elastic4s" % "elastic4s-core_2.12" % elastic4sVersion,

  // for the http client
  "com.sksamuel.elastic4s" % "elastic4s-http_2.12" % elastic4sVersion,

  // testing
  "com.sksamuel.elastic4s" % "elastic4s-testkit_2.12" %  elastic4sVersion % "test",
  "com.sksamuel.elastic4s" % "elastic4s-tests_2.12" % elastic4sVersion % "test"
)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
