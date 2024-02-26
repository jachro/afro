package io.renku.avro4s

object all
    extends PrimitiveTypeEncoders
    with PrimitiveTypeDecoders
    with EnumTypeEncoder
    with EnumTypeDecoder
    with CollectionEncoders
    with CollectionDecoders
