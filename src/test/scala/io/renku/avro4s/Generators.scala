package io.renku.avro4s

import org.scalacheck.{Arbitrary, Gen}

object Generators extends Generators

trait Generators:

  extension [T](gen: Gen[T])
    def generateOne: T = gen.sample.getOrElse(generateOne)
    def generateList(size: Int): List[T] = Gen.listOfN(size, gen).generateOne

  extension [T](arb: Arbitrary[T])
    def generateOne: T = arb.arbitrary.generateOne
    def generateList(size: Int): List[T] = arb.arbitrary.generateList(size)
