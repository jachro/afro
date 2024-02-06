import sbt.*

//noinspection TypeAnnotation
object Dependencies {

  object V {
    val avro = "1.11.3"
    val catsCore = "2.10.0"
    val fs2 = "3.9.4"
    val scalacheck = "1.17.0"
    val scalatest = "3.2.17"
    val scalatestScalacheck = "3.2.14.0"
    val scodec = "2.2.2"
  }

  val avro = Seq(
    "org.apache.avro" % "avro" % V.avro
  )

  val scodec = Seq(
    "org.scodec" %% "scodec-core" % V.scodec
  )

  val catsCore = Seq(
    "org.typelevel" %% "cats-core" % V.catsCore
  )

  val catsFree = Seq(
    "org.typelevel" %% "cats-free" % V.catsCore
  )

  val fs2Core = Seq(
    "co.fs2" %% "fs2-core" % V.fs2
  )

  val scalacheck = Seq(
    "org.scalacheck" %% "scalacheck" % V.scalacheck
  )

  val scalatest = Seq(
    "org.scalatest" %% "scalatest" % V.scalatest
  )

  val scalatestScalaCheck = Seq(
    "org.scalatestplus" %% "scalacheck-1-16" % V.scalatestScalacheck
  )
}
