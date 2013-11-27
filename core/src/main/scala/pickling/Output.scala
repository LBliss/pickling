package scala.pickling

trait Output[T] {

  def result(): T

  def put(obj: T): this.type

}

// and then demand Output[Nothing] in the abstract PickleFormat
// in JSON we can demand Output[String], since Output[Nothing] <: Output[String]


trait ArrayOutput[T] extends Output[Array[T]] {
  // Put a single T
  def +=(obj: T): Unit
  
}

import scala.collection.mutable.ArrayBuffer

class ByteArrayBufferOutput extends ArrayOutput[Byte] {

  private val buf =
    ArrayBuffer[Byte]()
    
  def result(): Array[Byte] =
    buf.toArray
  
  def +=(obj: Byte) =
    buf += obj
  
  def put(obj: Array[Byte]): this.type = {
    buf ++= obj
    this
  }
  
}

class ByteArrayOutput(len: Int) extends ArrayOutput[Byte]  {

  private var pos = 0
  private val arr = Array.ofDim[Byte](len)
  
  def result(): Array[Byte] =
    arr
  
  def +=(obj: Byte) = {
    arr(pos) = obj
    pos = pos + 1
  }
  
  def put(obj: Array[Byte]): this.type = {
    Array.copy(obj, 0, arr, pos, obj.length)
    //binary.Util.UnsafeMemory.unsafe.copyMemory(obj, srcOffset, arr, destOffset + i + 4, obj.length * 1)
    pos = pos + obj.length
    this
  }
  
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
