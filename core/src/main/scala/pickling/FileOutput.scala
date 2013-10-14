package scala.pickling
package io

import java.io.{ File, PrintWriter }
import scala.pickling.binary.ByteArray
import java.io.FileOutputStream

class TextFileOutput(file: File) extends Output[String] {

  private val writer = new PrintWriter(file)

  def result(): String =
    ???

  def put(obj: String): this.type = {
    writer.print(obj)
    this
  }

  def close(): Unit = writer.close()

}

class BinaryFileEncodedOutput(file: File) extends EncodingOutput[Array[Byte]] {

  private val writer = new FileOutputStream(file)

  def result(): Array[Byte] =
    ???
    
  def put(obj: Array[Byte]): this.type = {
    writer.write(obj)
    this
  }
  
  def encodeByteTo(pos: Int, value: Byte): Int = {
    val array = new Array[Byte](1)
    val newpos = (new ByteArray(array)).encodeByteTo(0, value)
    writer.write(array)
    newpos + pos
  }
  def encodeByteAtEnd(pos: Int, value: Byte): Unit = {
    val array = new Array[Byte](1)
    (new ByteArray(array)).encodeByteAtEnd(0, value)
    writer.write(array)
  }
  def encodeShortAtEnd(pos: Int, value: Short): Unit = {
    val array = new Array[Byte](2)
    (new ByteArray(array)).encodeShortAtEnd(0, value)
    writer.write(array)
  }
  def encodeCharAtEnd(pos: Int, value: Char): Unit = {
    val array = new Array[Byte](4)
    (new ByteArray(array)).encodeCharAtEnd(0, value)
    writer.write(array)
  }
  def encodeIntAtEnd(pos: Int, value: Int): Unit = {
    val array = new Array[Byte](4)
    (new ByteArray(array)).encodeIntAtEnd(0, value)
    writer.write(array)
  }
  def encodeLongAtEnd(pos: Int, value: Long): Unit = {
    val array = new Array[Byte](8)
    (new ByteArray(array)).encodeLongAtEnd(0, value)
    writer.write(array)
  }
  def encodeIntTo(pos: Int, value: Int): Int = {
    val array = new Array[Byte](4)
    val newpos = (new ByteArray(array)).encodeIntTo(0, value)
    writer.write(array)
    newpos + pos
  }
  def encodeStringTo(pos: Int, value: String): Int = {
    val array = new Array[Byte](value.getBytes("UTF-8").length + 4)
    val newpos = (new ByteArray(array)).encodeStringTo(0, value)
    writer.write(array)
    newpos + pos
  }
  def encodeBooleanTo(pos: Int, value: Boolean): Int = {
    val array = new Array[Byte](1)
    val newpos = (new ByteArray(array)).encodeBooleanTo(0, value)
    writer.write(array)
    newpos + pos
  }
  def encodeByteArrayTo(pos: Int, ia: Array[Byte]): Int = {
    val array = new Array[Byte](ia.length + 4)
    val newpos = (new ByteArray(array)).encodeByteArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeShortArrayTo(pos: Int, ia: Array[Short]): Int = {
    val array = new Array[Byte](ia.length * 2 + 4)
    val newpos = (new ByteArray(array)).encodeShortArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeCharArrayTo(pos: Int, ia: Array[Char]): Int = {
    val array = new Array[Byte](ia.length * 4 + 4)
    val newpos = (new ByteArray(array)).encodeCharArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeIntArrayTo(pos: Int, ia: Array[Int]): Int = {
    val array = new Array[Byte](ia.length * 4 + 4)
    val newpos = (new ByteArray(array)).encodeIntArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeLongArrayTo(pos: Int, ia: Array[Long]): Int = {
    val array = new Array[Byte](ia.length * 8 + 4)
    val newpos = (new ByteArray(array)).encodeLongArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeBooleanArrayTo(pos: Int, ia: Array[Boolean]): Int = {
    val array = new Array[Byte](ia.length + 4)
    val newpos = (new ByteArray(array)).encodeBooleanArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeFloatArrayTo(pos: Int, ia: Array[Float]): Int = {
    val array = new Array[Byte](ia.length * 4 + 4)
    val newpos = (new ByteArray(array)).encodeFloatArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def encodeDoubleArrayTo(pos: Int, ia: Array[Double]): Int = {
    val array = new Array[Byte](ia.length * 8 + 4)
    val newpos = (new ByteArray(array)).encodeDoubleArrayTo(0, ia)
    writer.write(array)
    newpos + pos
  }
  def copyTo(pos: Int, bytes: Array[Byte]): Int = {
    val array = new Array[Byte](bytes.length)
    val newpos = (new ByteArray(array)).copyTo(0, bytes)
    writer.write(array)
    newpos + pos
  }

  def close(): Unit = writer.close()

}
