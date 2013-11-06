package scala.pickling

import java.io.{File, PrintWriter, FileOutputStream}
import scala.collection.mutable.ArrayBuffer

trait Output[T] {

  def result(): T

  def put(obj: T): this.type

}

// and then demand Output[Nothing] in the abstract PickleFormat
// in JSON we can demand Output[String], since Output[Nothing] <: Output[String]


// encoded primitives: Byte, Short, Char, Int, Long, Boolean, String
// encoded primitive arrays: Array[Byte], Array[Int]
// Q: why do the "AtEnd" methods also take a "pos"? Isn't it always ignored?

trait EncodingOutput[T] extends Output[T] {
  def encodeByteTo(pos: Int, value: Byte): Int
  def encodeByteAtEnd(pos: Int, value: Byte): Unit
  def encodeShortAtEnd(pos: Int, value: Short): Unit
  def encodeCharAtEnd(pos: Int, value: Char): Unit
  def encodeIntAtEnd(pos: Int, value: Int): Unit
  def encodeLongAtEnd(pos: Int, value: Long): Unit
  def encodeIntTo(pos: Int, value: Int): Int
  def encodeStringTo(pos: Int, value: String): Int
  def encodeBooleanTo(pos: Int, value: Boolean): Int
  def encodeByteArrayTo(pos: Int, ia: Array[Byte]): Int
  def encodeShortArrayTo(pos: Int, ia: Array[Short]): Int
  def encodeCharArrayTo(pos: Int, ia: Array[Char]): Int
  def encodeIntArrayTo(pos: Int, ia: Array[Int]): Int
  def encodeLongArrayTo(pos: Int, ia: Array[Long]): Int
  def encodeBooleanArrayTo(pos: Int, ia: Array[Boolean]): Int
  def encodeFloatArrayTo(pos: Int, ia: Array[Float]): Int
  def encodeDoubleArrayTo(pos: Int, ia: Array[Double]): Int
  def copyTo(pos: Int, bytes: Array[Byte]): Int
}

class StringOutput extends Output[String] {

  private val buf =
    new StringBuilder()

  def result(): String =
    buf.toString

  def put(obj: String): this.type = {
    buf ++= obj
    this
  }

  override def toString = buf.toString

}

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

/*class ByteArrayOutput(arr: Array[Byte]) extends Output[Array[Byte]]  {

  private var pos = 0

  def this(size: Int) {
    this(Array.ofDim[Byte](size))
  }

  def result(): Array[Byte] =
    arr

  def put(obj: Array[Byte]): this.type = {
    assert(obj.length + pos <= arr.length)
    Array.copy(obj, 0, arr, pos, obj.length)
    pos += obj.length
    this
  }
}

class ArrayBufferOutput extends Output[Array[Byte]] {

  private var pos = 0
  private val buf = ArrayBuffer[Byte]()

  def result(): Array[Byte] =
    buf.toArray

  def put(obj: Array[Byte]): this.type = {
    buf ++= obj
    this
  }
}

class BinaryFileOutput(file: File) extends Output[Array[Byte]] {

  private val writer = new FileOutputStream(file)

  def result(): Array[Byte] =
    ???

  def put(obj: Array[Byte]): this.type = {
    writer.write(obj)
    this
  }

  def close(): Unit = writer.close()
  
}
*/