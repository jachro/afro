package io.jachro.afro

import cats.syntax.all.*

trait MapEncoders extends CollectionEncoders with PrimitiveTypeEncoders:

  given mapEncoder[M[X] <: collection.Map[String, X], I](using
      ve: TypeEncoder[I]
  ): TypeEncoder[M[I]] =
    given TypeEncoder[(String, I)] = entryEncoder[I]
    TypeEncoder
      .instance[List[(String, I)]](encodeList[(String, I)])
      .contramap[M[I]](_.toList)

  def blockingMapEncoder[M[X] <: collection.Map[String, X], I](
      blockSize: Int = defaultBinaryBlockSize
  )(using ie: TypeEncoder[I]): TypeEncoder[M[I]] =
    given TypeEncoder[(String, I)] = entryEncoder[I]
    TypeEncoder
      .instance[List[(String, I)]](encodeBlockingList[(String, I)](blockSize))
      .contramap[M[I]](_.toList)

  private def entryEncoder[I]: TypeEncoder[I] ?=> TypeEncoder[(String, I)] =
    ve ?=>
      TypeEncoder.instance[(String, I)] { case (k, v) =>
        (TypeEncoder[String].encodeValue(k), ve.encodeValue(v))
          .mapN(_ ++ _)
      }
