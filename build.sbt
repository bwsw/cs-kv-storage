val ScalatraVersion = "2.6.3"

organization := "com.bwsw"

name := "KV Storage"

version := "0.1"

scalaVersion := "2.12.6"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.4.9.v20180320" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.2.4",
  "com.typesafe.akka" %% "akka-actor" % "2.5.12",
  "org.scalamock" %% "scalamock" % "4.1.0" % Test
)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
