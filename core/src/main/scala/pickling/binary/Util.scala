package scala.pickling

package binary {
  import collection.mutable.Buffer
  import UnsafeMemory._

  object Util {
    
    /** Returns decoded Short plus next "readable" position in target array.
     */
    def decodeShort(arr: Array[Byte]): Short = {
      val fst = ((arr(0) << 8) & 0xFFFF).toShort
      val snd = (arr(1)      & 0x00FF).toShort
      (fst | snd).toShort
    }

    /** Returns decoded Char.
     */
    def decodeChar(arr: Array[Byte]): Char = {
      val fst = ((arr(0) << 8) & 0xFFFF).toChar
      val snd = (arr(1)      & 0x00FF).toChar
      (fst | snd).toChar
    }

    /** Returns decoded Int.
     */
    def decodeInt(arr: Array[Byte]): Int = {
      val fst = (arr(0) << 24).toInt
      val snd = ((arr(1) << 16) & 0x00FFFFFF).toInt
      val thrd = ((arr(2) << 8) & 0x0000FFFF).toInt
      val frth = (arr(3) & 0x000000FF).toInt
      fst | snd | thrd | frth
    }

    /** Returns decoded Long.
     */
    def decodeLong(arr: Array[Byte]): Long = {
      val elem1 = ((arr(0).toLong   << 56) & 0xFFFFFFFFFFFFFFFFL).toLong
      val elem2 = ((arr(1).toLong << 48) & 0x00FFFFFFFFFFFFFFL).toLong
      val elem3 = ((arr(2).toLong << 40) & 0x0000FFFFFFFFFFFFL).toLong
      val elem4 = ((arr(3).toLong << 32) & 0x000000FFFFFFFFFFL).toLong
      val elem5 = ((arr(4).toLong << 24) & 0x00000000FFFFFFFFL).toLong
      val elem6 = ((arr(5).toLong << 16) & 0x0000000000FFFFFFL).toLong
      val elem7 = ((arr(6).toLong << 8)  & 0x000000000000FFFFL).toLong
      val elem8 = (arr(7).toLong         & 0x00000000000000FFL).toLong
      elem1 | elem2 | elem3 | elem4 | elem5 | elem6 | elem7 | elem8
    }

    /** Returns the array.
     */
    def encodeByte(arr: Array[Byte], value: Byte): Array[Byte] = {
      arr(0) = value
      arr
    }
    
    /** Returns the array.
     */
    def encodeShort(arr: Array[Byte], value: Short): Array[Byte] = {
      val fst = (value >>> 8 & 0xff).asInstanceOf[Byte]
      val snd = (value & 0xff).asInstanceOf[Byte]
      arr(0) = fst
      arr(1) = snd
      arr
    }

    /** Returns the array.
     */
    def encodeChar(arr: Array[Byte], value: Char): Array[Byte] = {
      val fst = (value >>> 8 & 0xff).asInstanceOf[Byte]
      val snd = (value & 0xff).asInstanceOf[Byte]
      arr(0) = fst
      arr(1) = snd
      arr
    }

    /** Returns the array.
     */
    def encodeInt(arr: Array[Byte], value: Int): Array[Byte] = {
      val fst = (value >>> 24).asInstanceOf[Byte]
      val snd = (value >>> 16 & 0xff).asInstanceOf[Byte]
      val thrd = (value >>> 8 & 0xff).asInstanceOf[Byte]
      val frth = (value & 0xff).asInstanceOf[Byte]
      arr(0) = fst
      arr(1) = snd
      arr(2) = thrd
      arr(3) = frth
      arr
    }

    /** Returns the array.
     */
    def encodeLong(arr: Array[Byte], value: Long): Array[Byte] = {
      val elem1 = (value >>> 56 & 0xff).asInstanceOf[Byte]
      val elem2 = (value >>> 48 & 0xff).asInstanceOf[Byte]
      val elem3 = (value >>> 40 & 0xff).asInstanceOf[Byte]
      val elem4 = (value >>> 32 & 0xff).asInstanceOf[Byte]
      val elem5 = (value >>> 24 & 0xff).asInstanceOf[Byte]
      val elem6 = (value >>> 16 & 0xff).asInstanceOf[Byte]
      val elem7 = (value >>> 8 & 0xff).asInstanceOf[Byte]
      val elem8 = (value & 0xff).asInstanceOf[Byte]
      arr(0) = elem1
      arr(1) = elem2
      arr(2) = elem3
      arr(3) = elem4
      arr(4) = elem5
      arr(5) = elem6
      arr(6) = elem7
      arr(7) = elem8
      arr
    }
    
    /** Returns the array.
     */
    def encodeBoolean(arr: Array[Byte], value: Boolean): Array[Byte] = {
      arr(0) = if (value) 1 else 0
      arr
    }

  }

  object UnsafeMemory {
    import sun.misc.Unsafe

    private[binary] val unsafe: Unsafe =
      scala.concurrent.util.Unsafe.instance

    private[binary] val byteArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Byte]])
    private[binary] val shortArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Short]])
    private[binary] val charArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Char]])
    private[binary] val intArrayOffset: Long  = unsafe.arrayBaseOffset(classOf[Array[Int]])
    private[binary] val longArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Long]])
    private[binary] val booleanArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Boolean]])
    private[binary] val floatArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Float]])
    private[binary] val doubleArrayOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Double]])

  }

}
