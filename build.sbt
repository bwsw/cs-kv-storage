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
  "com.typesafe" % "config" % "1.3.3"
)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
