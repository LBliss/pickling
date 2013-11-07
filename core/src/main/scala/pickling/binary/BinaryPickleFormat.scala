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
    type InputType = BinaryInput // Input[Array[Byte]] with side-effect reads
    type OutputType = Output[Array[Byte]]
    
    def createBuilder() = new BinaryPickleBuilder(this, null)
    def createBuilder(out: Output[Array[Byte]]): PBuilder = new BinaryPickleBuilder(this, out)
    def createReader(pickle: PickleType, mirror: Mirror) = new BinaryStreamReader(new ByteArrayInput(pickle.value), mirror, this)
    def createReader(input: InputType, mirror: Mirror) = new BinaryStreamReader(input, mirror, this)
  }
  
  final class BinaryPickleBuilder(format: BinaryPickleFormat, output: Output[Array[Byte]]) extends PBuilder with PickleTools {
    import format._

    private var out: Output[Array[Byte]] = output
    private val buffer1: Array[Byte] = new Array[Byte](1)
    private val buffer2: Array[Byte] = new Array[Byte](2)
    private val buffer4: Array[Byte] = new Array[Byte](4)
    private val buffer8: Array[Byte] = new Array[Byte](8)
    private var bufferN: Array[Byte] = null
    
    @inline private[this] def mkOutput(knownSize: Int): Unit =
      if (out == null) {
        out = if (knownSize != -1) new ByteArrayOutput(knownSize) else new ArrayBufferOutput
      }

    @inline def beginEntry(picklee: Any): PBuilder = withHints { hints =>
      mkOutput(hints.knownSize)
      if (picklee == null) {
        out.put(Util.encodeByte(buffer1, NULL_TAG))
      } else if (hints.oid != -1) {
        out.put(Util.encodeByte(buffer1, REF_TAG))
        out.put(Util.encodeInt(buffer4, hints.oid))
      } else {
        if (!hints.isElidedType) {
          val tpeBytes = hints.tag.key.getBytes("UTF-8")
          out.put(Util.encodeInt(buffer4, tpeBytes.length))
          out.put(tpeBytes)
        }

        hints.tag.key match { // PERF: should store typestring once in hints.
          case KEY_NULL =>
            out.put(Util.encodeByte(buffer1, NULL_TAG))
          case KEY_BYTE =>
            out.put(Util.encodeByte(buffer1, picklee.asInstanceOf[Byte]))
          case KEY_SHORT =>
            out.put(Util.encodeShort(buffer2, picklee.asInstanceOf[Short]))
          case KEY_CHAR =>
            out.put(Util.encodeChar(buffer2, picklee.asInstanceOf[Char]))
          case KEY_INT =>
            out.put(Util.encodeInt(buffer4, picklee.asInstanceOf[Int]))
          case KEY_LONG =>
            out.put(Util.encodeLong(buffer8, picklee.asInstanceOf[Long]))
          case KEY_BOOLEAN =>
            out.put(Util.encodeBoolean(buffer1, picklee.asInstanceOf[Boolean]))
          case KEY_FLOAT =>
            val intValue = java.lang.Float.floatToRawIntBits(picklee.asInstanceOf[Float])
            out.put(Util.encodeInt(buffer4, intValue))
          case KEY_DOUBLE =>
            val longValue = java.lang.Double.doubleToRawLongBits(picklee.asInstanceOf[Double])
            out.put(Util.encodeLong(buffer8, longValue))
          case KEY_SCALA_STRING | KEY_JAVA_STRING =>
            val bytes = picklee.asInstanceOf[String].getBytes("UTF-8")
            out.put(Util.encodeInt(buffer4, bytes.length))
            out.put(bytes)
          case KEY_ARRAY_BYTE =>
            val src = picklee.asInstanceOf[Array[Byte]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.byteArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_CHAR =>
            val src = picklee.asInstanceOf[Array[Char]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*4)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.charArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_SHORT =>
            val src = picklee.asInstanceOf[Array[Short]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*2)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.shortArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_INT =>
            val src = picklee.asInstanceOf[Array[Int]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*4)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.intArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_LONG =>
            val src = picklee.asInstanceOf[Array[Long]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*8)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.longArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_BOOLEAN =>
            val src = picklee.asInstanceOf[Array[Boolean]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.booleanArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_FLOAT =>
            val src = picklee.asInstanceOf[Array[Float]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*4)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.floatArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case KEY_ARRAY_DOUBLE =>
            val src = picklee.asInstanceOf[Array[Double]]
            out.put(Util.encodeInt(buffer4, src.length))
            bufferN = new Array[Byte](src.length*8)
            UnsafeMemory.unsafe.copyMemory(src, UnsafeMemory.doubleArrayOffset, bufferN, UnsafeMemory.byteArrayOffset, bufferN.length)
            out.put(bufferN)
          case _ =>
            if (hints.isElidedType) out.put(Util.encodeByte(buffer1, ELIDED_TAG))
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

    @inline def beginCollection(length: Int): PBuilder = {
      out.put(Util.encodeInt(buffer4, length))
      this
    }

    @inline def putElement(pickler: PBuilder => Unit): PBuilder = {
      pickler(this)
      this
    }

    @inline def endCollection(length: Int): Unit = {
      // TODO: Length may change between begin and end... this is bad
      // byteBuffer.encodeIntTo(localBeginCollPos, length) // Really?
    }

    @inline def result() = {
      BinaryPickle(out.result)
    }
  }

  final class BinaryStreamReader(input: BinaryInput, val mirror: Mirror, format: BinaryPickleFormat) extends PReader with PickleTools {
    import format._

    private val buffer2: Array[Byte] = new Array[Byte](2)
    private val buffer4: Array[Byte] = new Array[Byte](4)
    private val buffer8: Array[Byte] = new Array[Byte](8)
    private var bufferN: Array[Byte] = null
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
      val len = Util.decodeInt(buffer4)
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
      } else {
        ???
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
        streamRead(4); lookupUnpicklee(Util.decodeInt(buffer4))
      case KEY_BYTE =>
        byteRead
      case KEY_SHORT =>
        streamRead(2); Util.decodeShort(buffer2)
      case KEY_CHAR =>
        streamRead(2); Util.decodeChar(buffer2)
      case KEY_INT =>
        streamRead(4); Util.decodeInt(buffer4)
      case KEY_LONG =>
        streamRead(8); Util.decodeLong(buffer8)
      case KEY_BOOLEAN =>
        byteRead != 0
      case KEY_FLOAT =>
        streamRead(4)
        val r = Util.decodeInt(buffer4)
        java.lang.Float.intBitsToFloat(r)
      case KEY_DOUBLE =>
        streamRead(8)
        val r = Util.decodeInt(buffer4)
        java.lang.Double.longBitsToDouble(r)

      case KEY_SCALA_STRING | KEY_JAVA_STRING =>
        decodeString()

      case KEY_ARRAY_BYTE =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 1)
        input.read(bufferN)
        val ia = Array.ofDim[Byte](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.byteArrayOffset, len * 1)
        ia
      case KEY_ARRAY_SHORT =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 2)
        input.read(bufferN)
        val ia = Array.ofDim[Short](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.shortArrayOffset, len * 2)
        ia
      case KEY_ARRAY_CHAR =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Char](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.charArrayOffset, len * 4)
        ia
      case KEY_ARRAY_INT =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Int](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.intArrayOffset, len * 4)
        ia
      case KEY_ARRAY_LONG =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 8)
        input.read(bufferN)
        val ia = Array.ofDim[Long](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.longArrayOffset, len * 8)
        ia
      case KEY_ARRAY_BOOLEAN =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 1)
        input.read(bufferN)
        val ia = Array.ofDim[Boolean](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.booleanArrayOffset, len * 1)
        ia
      case KEY_ARRAY_FLOAT =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 4)
        input.read(bufferN)
        val ia = Array.ofDim[Float](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.floatArrayOffset, len * 4)
        ia
      case KEY_ARRAY_DOUBLE =>
        streamRead(4)
        val len = Util.decodeInt(buffer4)
        bufferN = new Array[Byte](len * 8)
        input.read(bufferN)
        val ia = Array.ofDim[Double](len)
        UnsafeMemory.unsafe.copyMemory(bufferN, UnsafeMemory.byteArrayOffset, ia, UnsafeMemory.doubleArrayOffset, len * 8)
        ia
    }

    def atObject: Boolean = !atPrimitive

    def readField(name: String): BinaryStreamReader =
      this

    def endEntry(): Unit = { /* do nothing */ }

    def beginCollection(): PReader = this

    def readLength(): Int = {
      streamRead(4)
      Util.decodeInt(buffer4)
    }

    def readElement(): PReader = this

    def endCollection(): Unit = { /* do nothing */ }
  }
  
 }
