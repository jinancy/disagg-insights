package com.oracle.ugbu.ouai.insight.utils

import org.slf4j.LoggerFactory

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

case class CryptoUtils(key : String)  {
  private val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  // AES only supports key sizes of 16, 24 or 32 bytes.
  private val secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
  private val encoder = Base64.getUrlEncoder()
  private val decoder = Base64.getUrlDecoder()

  def encrypt(plainText: String): String = {
    try { // Get byte array which has to be encrypted.
      val plainTextByte = plainText.getBytes("UTF-8")
      // Encrypt the bytes using the secret key
      val cipher = Cipher.getInstance("AES")
      cipher.init(Cipher.ENCRYPT_MODE, secretKey)
      val encryptedByte = cipher.doFinal(plainTextByte)
      // Use Base64 encoder to encode the byte array
      // into Base 64 representation. Requires Java 8.
      return encoder.encodeToString(encryptedByte)
    } catch {
      case e: Exception =>
        logger.error("Failed to encrypt", e)
        throw e
    }
  }

  def decrypt(encrypted : String) : String = {
    try {
      // Decode Base 64 String into bytes array.
      val encryptedByte = decoder.decode(encrypted)

      //Do the decryption
      val cipher = Cipher.getInstance("AES")
      cipher.init(Cipher.DECRYPT_MODE, secretKey)
      val decryptedByte = cipher.doFinal(encryptedByte)

      // Get hexadecimal string from the byte array.
      new String(decryptedByte,"UTF-8")
    }
    catch {
      case e : Exception => {
        logger.error("Failed to decrypt", e)
        throw e
      }
    }
  }
}
