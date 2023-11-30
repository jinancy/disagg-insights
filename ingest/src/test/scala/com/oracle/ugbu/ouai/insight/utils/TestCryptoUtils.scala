package com.oracle.ugbu.ouai.insight.utils

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestCryptoUtils extends FunSuite {
  val textToEncode = "encoding_plain_text"
  val secretKey = "secret_key_123secret_key" //24 bytes
  val cryptoUtils = CryptoUtils(secretKey)
  val encoded = "fGE-PQugHB_BmCpN7zExi2t4Q9FjzEqSdZDKMvxPZNw="

  test("test encoding") {
    assert(cryptoUtils.encrypt(textToEncode) == encoded)
  }

  test("test decoding") {
    assert(cryptoUtils.decrypt(encoded) == textToEncode)
  }

}

