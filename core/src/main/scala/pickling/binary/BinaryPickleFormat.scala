package scala.pickling

import scala.pickling.internal._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.Mirror

package object binary {
  implicit val pickleFormat = new BinaryPickleFormat
  implicit def toBinaryPickle(value: Array[Byte]): BinaryPickle = BinaryPickle(value)
}

package binary {

  case class BinaryPickle(value: Array[Byte]) extends Pickle {
    type ValueType = Array[Byte]
    type PickleFormatType = BinaryPickleFormat
    override def toString = s"""BinaryPickle(${value.mkString("[", ",", "]")})"""
  }

  final class BinaryPickleBuilder(format: BinaryPickleFormat, out: EncodingOutput[Array[Byte]]) extends PBuilder with PickleTools {
    import format._

    private var BinaryEncoder: EncodingOutput[Array[Byte]] =
      out.asInstanceOf[EncodingOutput[Array[Byte]]]

    private var pos = 0

    @inline private[this] def mkBinaryEncoder(knownSize: Int): Unit =
      if (BinaryEncoder == null) {
        BinaryEncoder = if (knownSize != -1) new ByteArrayBinaryEncoder(knownSize) else new ArrayBufferBinaryEncoder
      }

    @inline def beginEntry(picklee: Any): PBuilder = withHints { hints =>
      mkBinaryEncoder(hints.knownSize)

      if (picklee == null) {
        pos = BinaryEncoder.encodeByteTo(pos, NULL_TAG)
      } else if (hints.oid != -1) {
        BinaryEncoder.encodeByteTo(pos, REF_TAG)
        BinaryEncoder.encodeIntAtEnd(pos + 1, hints.oid)
        pos = pos + 5
      } else {
        if (!hints.isElidedType) {
          val tpeBytes = hints.tag.key.getBytes("UTF-8")
          BinaryEncoder.encodeIntAtEnd(pos, tpeBytes.length)
          pos += 4
          pos = BinaryEncoder.copyTo(pos, tpeBytes)
        }

        // NOTE: it looks like we don't have to write object ids at all
        // traversals employed by pickling and unpickling are exactly the same
        // hence when unpickling it's enough to just increment the nextUnpicklee counter
        // and everything will work out automatically!

        pos = hints.tag.key match { // PERF: should store typestring once in hints.
          case KEY_NULL =>
            BinaryEncoder.encodeByteTo(pos, NULL_TAG)
          case KEY_BYTE =>
            BinaryEncoder.encodeByteAtEnd(pos, picklee.asInstanceOf[Byte])
            pos + 1
          case KEY_SHORT =>
            BinaryEncoder.encodeShortAtEnd(pos, picklee.asInstanceOf[Short])
            pos + 2
          case KEY_CHAR =>
            BinaryEncoder.encodeCharAtEnd(pos, picklee.asInstanceOf[Char])
            pos + 2
          case KEY_INT =>
            BinaryEncoder.encodeIntAtEnd(pos, picklee.asInstanceOf[Int])
            pos + 4
          case KEY_LONG =>
            BinaryEncoder.encodeLongAtEnd(pos, picklee.asInstanceOf[Long])
            pos + 8
          case KEY_BOOLEAN =>
            BinaryEncoder.encodeBooleanTo(pos, picklee.asInstanceOf[Boolean])
          case KEY_FLOAT =>
            val intValue = java.lang.Float.floatToRawIntBits(picklee.asInstanceOf[Float])
            BinaryEncoder.encodeIntAtEnd(pos, intValue)
            pos + 4
          case KEY_DOUBLE =>
            val longValue = java.lang.Double.doubleToRawLongBits(picklee.asInstanceOf[Double])
            BinaryEncoder.encodeLongAtEnd(pos, longValue)
            pos + 8
          case KEY_SCALA_STRING | KEY_JAVA_STRING =>
            BinaryEncoder.encodeStringTo(pos, picklee.asInstanceOf[String])
          case KEY_ARRAY_BYTE =>
            BinaryEncoder.encodeByteArrayTo(pos, picklee.asInstanceOf[Array[Byte]])
          case KEY_ARRAY_CHAR =>
            BinaryEncoder.encodeCharArrayTo(pos, picklee.asInstanceOf[Array[Char]])
          case KEY_ARRAY_SHORT =>
            BinaryEncoder.encodeShortArrayTo(pos, picklee.asInstanceOf[Array[Short]])
          case KEY_ARRAY_INT =>
            BinaryEncoder.encodeIntArrayTo(pos, picklee.asInstanceOf[Array[Int]])
          case KEY_ARRAY_LONG =>
            BinaryEncoder.encodeLongArrayTo(pos, picklee.asInstanceOf[Array[Long]])
          case KEY_ARRAY_BOOLEAN =>
            BinaryEncoder.encodeBooleanArrayTo(pos, picklee.asInstanceOf[Array[Boolean]])
          case KEY_ARRAY_FLOAT =>
            BinaryEncoder.encodeFloatArrayTo(pos, picklee.asInstanceOf[Array[Float]])
          case KEY_ARRAY_DOUBLE =>
            BinaryEncoder.encodeDoubleArrayTo(pos, picklee.asInstanceOf[Array[Double]])
          case _ =>
            if (hints.isElidedType) BinaryEncoder.encodeByteTo(pos, ELIDED_TAG)
            else pos
        }
      }

      this
    }

    @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = {
      // can skip writing name if we pickle/unpickle in the same order
      pickler(this)
      this
    }

    @inline def endEntry(): Unit = { /* do nothing */ }

    var beginCollPos = List[Int]()

    @inline def beginCollection(length: Int): PBuilder = {
      beginCollPos = pos :: beginCollPos
      BinaryEncoder.encodeIntAtEnd(pos, 0)
      pos += 4
      this
    }

    @inline def putElement(pickler: PBuilder => Unit): PBuilder = {
      pickler(this)
      this
    }

    @inline def endCollection(length: Int): Unit = {
      val localBeginCollPos = beginCollPos.head
      beginCollPos = beginCollPos.tail
      BinaryEncoder.encodeIntTo(localBeginCollPos, length)
    }

    @inline def result() = {
      BinaryPickle(BinaryEncoder.result())
    }
  }

  class BinaryStreamReader(input: ByteArrayInput, val mirror: Mirror, format: BinaryPickleFormat) extends PReader with PickleTools {
    import format._

    private val buffer2: Array[Byte] = new Array[Byte](2)
    private val buffer4: Array[Byte] = new Array[Byte](4)
    private val buffer8: Array[Byte] = new Array[Byte](8)
    private var bufferN: Array[Byte] = null
    private val srcOffset = UnsafeMemory.byteArrayOffset
    private val destOffset = UnsafeMemory.intArrayOffset
    private var lookaheaded: Option[Byte] = None

    private var _lastTagRead: FastTypeTag[_] = null
    private var _lastTypeStringRead: String = null

    private def lastTagRead: FastTypeTag[_] =
      if (_lastTagRead != null)
        _lastTagRead
      else {
        // assume _lastTypeStringRead != null
        _lastTagRead = FastTypeTag(mirror, _lastTypeStringRead)
        _lastTagRead
      }

    private def decodeString(): String = {
      streamRead(4)
      val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
      bufferN = new Array[Byte](len)
      input.read(bufferN)
      new String(bufferN, "UTF-8")
    }
    private def streamRead(x: Int) = {
      if (x == 2) {
        if (lookaheaded == None) {
          input.read(buffer2)
        } else {
          buffer2(0) = lookaheaded.getOrElse(0)
          buffer2(1) = input.readByte
          lookaheaded = None
        }
      } else if (x == 4) {
        if (lookaheaded == None) {
          input.read(buffer4)
        } else {
          buffer4(0) = lookaheaded.getOrElse(0)
          input.read(buffer4, 1, 3)
          lookaheaded = None
        }
      } else if (x == 8) {
        if (lookaheaded == None) {
          input.read(buffer8)
        } else {
          buffer8(0) = lookaheaded.getOrElse(0)
          input.read(buffer8, 1, 7)
          lookaheaded = None
        }
      }
    }
    private def byteRead: Byte = {
      if (lookaheaded == None) {
        input.readByte
      } else {
        val res: Byte = lookaheaded.getOrElse(0)
        lookaheaded = None
        res
      }
    }
    def beginEntryNoTag(): String = {
      val res: Any = withHints { hints =>
        if (hints.isElidedType && nullablePrimitives.contains(hints.tag.key)) {
          val lookahead = byteRead
          lookahead match {
            case NULL_TAG => FastTypeTag.Null
            case REF_TAG => FastTypeTag.Ref
            case _ => lookaheaded = Some(lookahead); hints.tag
          }
        } else if (hints.isElidedType && primitives.contains(hints.tag.key)) {
          hints.tag
        } else {
          val lookahead = byteRead
          lookahead match {
            case NULL_TAG =>
              FastTypeTag.Null
            case ELIDED_TAG =>
              hints.tag
            case REF_TAG =>
              FastTypeTag.Ref
            case _ =>
              lookaheaded = Some(lookahead)
              decodeString
          }
        }
      }
      if (res.isInstanceOf[String]) {
        _lastTagRead = null
        _lastTypeStringRead = res.asInstanceOf[String]
        _lastTypeStringRead
      } else {
        _lastTagRead = res.asInstanceOf[FastTypeTag[_]]
        _lastTagRead.key
      }
    }

    def beginEntry(): FastTypeTag[_] = {
      beginEntryNoTag()
      lastTagRead
    }

    def atPrimitive: Boolean = primitives.contains(lastTagRead.key)

    def readPrimitive(): Any = lastTagRead.key match {
      case KEY_NULL => null
      case KEY_REF =>
        streamRead(4); lookupUnpicklee((new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0))
      case KEY_BYTE =>
        byteRead
      case KEY_SHORT =>
        streamRead(2); (new ByteArrayBinaryEncoder(buffer2)).decodeShortFrom(0)
      case KEY_CHAR =>
        streamRead(2); (new ByteArrayBinaryEncoder(buffer2)).decodeCharFrom(0)
      case KEY_INT =>
        streamRead(4); (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
      case KEY_LONG =>
        streamRead(8); (new ByteArrayBinaryEncoder(buffer8)).decodeLongFrom(0)
      case KEY_BOOLEAN =>
        byteRead != 0
      case KEY_FLOAT =>
        streamRead(4)
        val r = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        java.lang.Float.intBitsToFloat(r)
      case KEY_DOUBLE =>
        streamRead(8)
        val r = (new ByteArrayBinaryEncoder(buffer8)).decodeLongFrom(0)
        java.lang.Double.longBitsToDouble(r)

      case KEY_SCALA_STRING | KEY_JAVA_STRING =>
        decodeString()

      case KEY_ARRAY_BYTE =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 1)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 1)
        ia
      case KEY_ARRAY_SHORT =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 2)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 2)
        ia
      case KEY_ARRAY_CHAR =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 4)
        ia
      case KEY_ARRAY_INT =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 4)
        ia
      case KEY_ARRAY_LONG =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 8)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 8)
        ia
      case KEY_ARRAY_BOOLEAN =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 1)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 1)
        ia
      case KEY_ARRAY_FLOAT =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 4)
        ia
      case KEY_ARRAY_DOUBLE =>
        streamRead(4)
        val len = (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
        bufferN = new Array[Byte](len * 8)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, srcOffset, ia, destOffset, len * 8)
        ia
    }

    def atObject: Boolean = !atPrimitive

    def readField(name: String): BinaryStreamReader =
      this

    def endEntry(): Unit = { /* do nothing */ }

    def beginCollection(): PReader = this

    def readLength(): Int = {
      streamRead(4)
      (new ByteArrayBinaryEncoder(buffer4)).decodeIntFrom(0)
    }

    def readElement(): PReader = this

    def endCollection(): Unit = { /* do nothing */ }
  }
  
  class BinaryPickleFormat extends PickleFormat {
    val ELIDED_TAG: Byte = -1
    val NULL_TAG: Byte = -2
    val REF_TAG: Byte = -3

    val KEY_NULL    = FastTypeTag.Null.key
    val KEY_BYTE    = FastTypeTag.Byte.key
    val KEY_SHORT   = FastTypeTag.Short.key
    val KEY_CHAR    = FastTypeTag.Char.key
    val KEY_INT     = FastTypeTag.Int.key
    val KEY_LONG    = FastTypeTag.Long.key
    val KEY_BOOLEAN = FastTypeTag.Boolean.key
    val KEY_FLOAT   = FastTypeTag.Float.key
    val KEY_DOUBLE  = FastTypeTag.Double.key
    val KEY_UNIT    = FastTypeTag.Unit.key

    val KEY_SCALA_STRING = FastTypeTag.ScalaString.key
    val KEY_JAVA_STRING  = FastTypeTag.JavaString.key

    val KEY_ARRAY_BYTE    = FastTypeTag.ArrayByte.key
    val KEY_ARRAY_SHORT   = FastTypeTag.ArrayShort.key
    val KEY_ARRAY_CHAR    = FastTypeTag.ArrayChar.key
    val KEY_ARRAY_INT     = FastTypeTag.ArrayInt.key
    val KEY_ARRAY_LONG    = FastTypeTag.ArrayLong.key
    val KEY_ARRAY_BOOLEAN = FastTypeTag.ArrayBoolean.key
    val KEY_ARRAY_FLOAT   = FastTypeTag.ArrayFloat.key
    val KEY_ARRAY_DOUBLE  = FastTypeTag.ArrayDouble.key

    val KEY_REF = FastTypeTag.Ref.key

    val primitives = Set(KEY_NULL, KEY_REF, KEY_BYTE, KEY_SHORT, KEY_CHAR, KEY_INT, KEY_LONG, KEY_BOOLEAN, KEY_FLOAT, KEY_DOUBLE, KEY_UNIT, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR, KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)
    val nullablePrimitives = Set(KEY_NULL, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR, KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)

    type PickleType = BinaryPickle
    type OutputType = EncodingOutput[Array[Byte]]
    def createBuilder() = new BinaryPickleBuilder(this, null)
    def createBuilder(out: EncodingOutput[Array[Byte]]): PBuilder = new BinaryPickleBuilder(this, out)
    // def createReader(pickle: PickleType, mirror: Mirror) = new BinaryPickleReader(pickle.value, mirror, this)
    def createReader(pickle: PickleType, mirror: Mirror) = new BinaryStreamReader(new ByteArrayInput(pickle.value), mirror, this)
  }
}
