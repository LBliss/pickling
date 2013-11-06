package scala.pickling.binary

import scala.collection.mutable.ArrayBuffer

sealed abstract class BinaryEncoder {
  def copyTo(pos: Int, bytes: Array[Byte]): Int

  def decodeByteFrom(pos: Int): Byte

  def decodeShortFrom(pos: Int): Short

  def decodeCharFrom(pos: Int): Char

  def decodeIntFrom(pos: Int): Int

  def decodeLongFrom(pos: Int): Long

  def decodeStringFrom(pos: Int): (String, Int)

  def decodeBooleanFrom(pos: Int): Boolean

  def decodeByteArrayFrom(pos: Int): (Array[Byte], Int)

  def decodeShortArrayFrom(pos: Int): (Array[Short], Int)

  def decodeCharArrayFrom(pos: Int): (Array[Char], Int)

  def decodeIntArrayFrom(pos: Int): (Array[Int], Int)

  def decodeLongArrayFrom(pos: Int): (Array[Long], Int)

  def decodeBooleanArrayFrom(pos: Int): (Array[Boolean], Int)

  def decodeFloatArrayFrom(pos: Int): (Array[Float], Int)

  def decodeDoubleArrayFrom(pos: Int): (Array[Double], Int)

  def toArray: Array[Byte]

  def result(): Array[Byte] =
    toArray

  // puts the byte array representing `obj` at the end of the buffer
  def put(obj: Array[Byte]): this.type = ???
}

/* Implementation of `BinaryEncoder` using fixed-size array.
 */
final class ByteArrayBinaryEncoder(arr: Array[Byte]) extends BinaryEncoder {

  def this(size: Int) {
    this(Array.ofDim[Byte](size))
  }
  
  def copyTo(pos: Int, bytes: Array[Byte]): Int = {
    Util.copy(arr, pos, bytes)
    pos + bytes.length
  }

  def decodeByteFrom(pos: Int): Byte = {
    arr(pos)
  }

  def decodeShortFrom(pos: Int): Short =
    Util.decodeShortFrom(arr, pos)

  def decodeCharFrom(pos: Int): Char =
    Util.decodeCharFrom(arr, pos)

  def decodeIntFrom(pos: Int): Int =
    Util.decodeIntFrom(arr, pos)

  def decodeLongFrom(pos: Int): Long =
    Util.decodeLongFrom(arr, pos)

  def decodeStringFrom(pos: Int): (String, Int) =
    Util.decodeStringFrom(arr, pos)

  def decodeBooleanFrom(pos: Int): Boolean =
    Util.decodeBooleanFrom(arr, pos)

  def decodeByteArrayFrom(pos: Int): (Array[Byte], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Byte](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 1)
    (ia, nextPos + len * 1)
  }

  def decodeShortArrayFrom(pos: Int): (Array[Short], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Short](len)
    val srcOffset = UnsafeMemory.shortArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 2)
    (ia, nextPos + len * 2)
  }

  def decodeCharArrayFrom(pos: Int): (Array[Char], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Char](len)
    val srcOffset = UnsafeMemory.charArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 4)
    (ia, nextPos + len * 4)
  }

  def decodeIntArrayFrom(pos: Int): (Array[Int], Int) = {
    // arr: Array[Byte]
    // 1. read length
    val len = decodeIntFrom(pos)
    val nextPos = pos+4

    // 2. allocate Array[Int] TODO: use Unsafe
    val ia = Array.ofDim[Int](len)

    // 3. the copy
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 4)

    (ia, nextPos + len * 4)
  }

  def decodeLongArrayFrom(pos: Int): (Array[Long], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Long](len)
    val srcOffset = UnsafeMemory.longArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 8)
    (ia, nextPos + len * 8)
  }

  def decodeBooleanArrayFrom(pos: Int): (Array[Boolean], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Boolean](len)
    val srcOffset = UnsafeMemory.booleanArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 1)
    (ia, nextPos + len * 1)
  }

  def decodeFloatArrayFrom(pos: Int): (Array[Float], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Float](len)
    val srcOffset = UnsafeMemory.floatArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 4)
    (ia, nextPos + len * 4)
  }

  def decodeDoubleArrayFrom(pos: Int): (Array[Double], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Double](len)
    val srcOffset = UnsafeMemory.doubleArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    UnsafeMemory.unsafe.copyMemory(arr, srcOffset + nextPos, ia, destOffset, len * 8)
    (ia, nextPos + len * 8)
  }

  def toArray: Array[Byte] =
    arr
}

/* Implementation of `BinaryEncoder` using `ArrayBuffer`.
 */
final class ArrayBufferBinaryEncoder extends BinaryEncoder {

  private val buf = ArrayBuffer[Byte]()
  
  def copyTo(pos: Int, bytes: Array[Byte]): Int = {
    // assume buf.size = buf
    buf ++= bytes
    pos + bytes.length
  }

  def decodeByteFrom(pos: Int): Byte = {
    buf(pos)
  }

  def decodeShortFrom(pos: Int): Short = {
    val fst = ((buf(pos) << 8) & 0xFFFF).toShort
    val snd = (buf(pos+1)      & 0x00FF).toShort
    (fst | snd).toShort
  }

  def decodeCharFrom(pos: Int): Char = {
    val fst = ((buf(pos) << 8) & 0xFFFF).toChar
    val snd = (buf(pos+1)      & 0x00FF).toChar
    (fst | snd).toChar
  }

  def decodeIntFrom(pos: Int): Int = {
    val fst = (buf(pos) << 24).toInt
    val snd = ((buf(pos+1) << 16) & 0x00FFFFFF).toInt
    val thrd = ((buf(pos+2) << 8) & 0x0000FFFF).toInt
    val frth = (buf(pos+3) & 0x000000FF).toInt
    fst | snd | thrd | frth
  }

  def decodeLongFrom(pos: Int): Long = {
    val elem1 = ((buf(pos) << 56)   & 0xFFFFFFFFFFFFFFFFL).toLong
    val elem2 = ((buf(pos+1) << 48) & 0xFFFFFFFFFFFFFFL).toLong
    val elem3 = ((buf(pos+2) << 40) & 0xFFFFFFFFFFFFL).toLong
    val elem4 = ((buf(pos+3) << 32) & 0xFFFFFFFFFFL).toLong
    val elem5 = ((buf(pos+4) << 24) & 0xFFFFFFFF).toLong
    val elem6 = ((buf(pos+5) << 16) & 0x00FFFFFF).toLong
    val elem7 = ((buf(pos+6) << 8) & 0x0000FFFF).toLong
    val elem8 = (buf(pos+7) & 0x000000FF).toLong
    elem1 | elem2 | elem3 | elem4 | elem5 | elem6 | elem7 | elem8
  }

  def decodeStringFrom(pos: Int): (String, Int) = {
    val len = decodeIntFrom(pos)
    println(s"decoding string of length $len, starting from ${pos+4}")
    val bytes = buf.slice(pos + 4, pos + 4 + len).toArray
    println("bytes: " + bytes.mkString(","))
    val res = new String(bytes, "UTF-8")
    println(s"result string: $res")
    (res, pos + 4 + len)
  }

  def decodeBooleanFrom(pos: Int): Boolean = {
    decodeByteFrom(pos) != 0
  }

  def decodeByteArrayFrom(pos: Int): (Array[Byte], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Byte](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 1)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 1)
    (ia, nextPos + len * 1)
  }

  def decodeShortArrayFrom(pos: Int): (Array[Short], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Short](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 2)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 2)
    (ia, nextPos + len * 2)
  }

  def decodeCharArrayFrom(pos: Int): (Array[Char], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Char](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 4)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 4)
    (ia, nextPos + len * 4)
  }

  def decodeIntArrayFrom(pos: Int): (Array[Int], Int) = {
    // buf: ArrayBuffer[Byte]
    // 1. read length
    val len = decodeIntFrom(pos)
    val nextPos = pos+4

    // 2. allocate Array[Int] TODO: use Unsafe
    val ia = Array.ofDim[Int](len)

    // 3. the copy
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    // read the required num of bytes from `buf`
    val newbuf = buf.slice(nextPos, nextPos + len * 4)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 4)

    (ia, nextPos + len * 4)
  }

  def decodeLongArrayFrom(pos: Int): (Array[Long], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Long](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 8)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 8)
    (ia, nextPos + len * 8)
  }

  def decodeBooleanArrayFrom(pos: Int): (Array[Boolean], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Boolean](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 1)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 1)
    (ia, nextPos + len * 1)
  }

  def decodeFloatArrayFrom(pos: Int): (Array[Float], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Float](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 4)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 4)
    (ia, nextPos + len * 4)
  }

  def decodeDoubleArrayFrom(pos: Int): (Array[Double], Int) = {
    val len = decodeIntFrom(pos)
    val nextPos = pos+4
    val ia = Array.ofDim[Double](len)
    val srcOffset = UnsafeMemory.byteArrayOffset
    val destOffset = UnsafeMemory.intArrayOffset
    val newbuf = buf.slice(nextPos, nextPos + len * 8)
    val ba: Array[Byte] = newbuf.toArray
    UnsafeMemory.unsafe.copyMemory(ba, srcOffset, ia, destOffset, len * 8)
    (ia, nextPos + len * 8)
  }

  def toArray: Array[Byte] =
    buf.toArray
}
