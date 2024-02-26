package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

trait CollectionEncoders extends PrimitiveTypeEncoders:

  given [I](using ie: TypeEncoder[I]): TypeEncoder[Array[I]] =
    TypeEncoder.instance[Array[I]] {
      case arr if arr.isEmpty => TypeEncoder[Long].encodeValue(0L)
      case arr =>
        for
          encLength <- TypeEncoder[Long].encodeValue(arr.length)
          encItems <- arr.foldLeft(encLength.asRight[AvroEncodingException]) {
            case (enc: Left[_, _], _) => enc
            case (Right(bv), i)       => ie.encodeValue(i).map(bv ++ _)
          }
        yield encItems :+ 0.toByte
    }
