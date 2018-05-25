val ScalatraVersion = "2.6.3"

organization := "com.bwsw"

name := "KV Storage"

version := "0.1"

scalaVersion := "2.12.6"

resolvers += Classpaths.typesafeReleases

val elastic4sVersion = "6.2.8"

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.4.9.v20180320" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "com.sksamuel.elastic4s" % "elastic4s-http_2.12" % elastic4sVersion,
  "com.typesafe" % "config" % "1.3.3",
  "com.typesafe.akka" %% "akka-actor" % "2.5.12",
  "org.scalatra" %% "scalatra-json" % ScalatraVersion,
  "org.json4s" %% "json4s-jackson" % "3.6.0-M4",

  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
