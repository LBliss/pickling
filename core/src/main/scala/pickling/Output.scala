package scala.pickling

import java.io.{File, PrintWriter, FileOutputStream}
import scala.collection.mutable.ArrayBuffer

trait Output[T] {

  def result(): T

  def put(obj: T): this.type

}

// and then demand Output[Nothing] in the abstract PickleFormat
// in JSON we can demand Output[String], since Output[Nothing] <: Output[String]

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

class ByteArrayOutput(arr: Array[Byte]) extends Output[Array[Byte]]  {

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
