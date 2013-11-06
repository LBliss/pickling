package scala.pickling

import scala.language.experimental.macros

import java.io.FileInputStream

trait Input[T] {

  // Side-effect read.
  def read(obj: T): Unit
  // Direct read.
  def read(): T
  
  // To unpickle from an Input.
  def unpickle[X] = macro Compat.UnpickleMacros_pickleUnpickle[X]
}

trait BinaryInput extends Input[Array[Byte]] {
  
  def read(obj: Array[Byte]): Unit
  
  def read(obj: Array[Byte], off: Int, len: Int): Unit
  
  def readByte(): Byte
  
  def read(): Array[Byte] = ???
}

class ByteArrayInput(value: Array[Byte]) extends BinaryInput {

  private var pos: Int = 0
  private val buf: Array[Byte] = value
  
  def read(obj: Array[Byte]): Unit = {
    assert(obj.length + pos <= value.length)
    Array.copy(buf, pos, obj, 0, obj.length)
    pos += obj.length
  }
  
  def read(obj: Array[Byte], off: Int, len: Int): Unit = {
    assert(obj.length + pos <= value.length)
    Array.copy(buf, pos, obj, off, len)
    pos += len
  }
  
  def readByte(): Byte = {
    pos += 1
    value(pos-1)
  }
  
}

class BinaryFileInput(fis: FileInputStream) extends BinaryInput {

  
  def read(obj: Array[Byte]): Unit = fis.read(obj)
  
  def read(obj: Array[Byte], off: Int, len: Int): Unit = fis.read(obj, off, len)
  
  def readByte(): Byte = fis.read.toByte
  
}