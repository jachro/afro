package io.renku.avro4s

import cats.syntax.all.*
import scodec.bits.ByteVector

import scala.reflect.ClassTag

trait CollectionEncoders extends PrimitiveTypeEncoders:

  given [C[X], I](using
      ie: TypeEncoder[I],
      converter: C[I] => Array[I],
      ict: ClassTag[I]
  ): TypeEncoder[C[I]] =
    encodeArray[I].contramap[C[I]](converter)

  given [I](using ct: ClassTag[Set[I]], ict: ClassTag[I]): Function1[Set[I], Array[I]] =
    _.toArray
  given [I](using ct: ClassTag[List[I]], ict: ClassTag[I]): Function1[List[I], Array[I]] =
    _.toArray

  private def encodeArray[I](using
      ie: TypeEncoder[I],
      ct: ClassTag[Array[I]]
  ): TypeEncoder[Array[I]] =
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
