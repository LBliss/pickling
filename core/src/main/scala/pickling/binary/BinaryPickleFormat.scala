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

  class BinaryPickleReader(arr: Array[Byte], val mirror: Mirror, format: BinaryPickleFormat) extends PReader with PickleTools {
    import format._

    private val BinaryEncoder: BinaryEncoder       = new ByteArrayBinaryEncoder(arr)
    private var pos                          = 0
    private var _lastTagRead: FastTypeTag[_] = null
    private var _lastTypeStringRead: String  = null

    private def lastTagRead: FastTypeTag[_] =
      if (_lastTagRead != null)
        _lastTagRead
      else {
        // assume _lastTypeStringRead != null
        _lastTagRead = FastTypeTag(mirror, _lastTypeStringRead)
        _lastTagRead
      }

    def beginEntryNoTag(): String = {
      val res: Any = withHints { hints =>
        if (hints.isElidedType && nullablePrimitives.contains(hints.tag.key)) {
          val lookahead = BinaryEncoder.decodeByteFrom(pos)
          lookahead match {
            case NULL_TAG => pos += 1; FastTypeTag.Null
            case REF_TAG  => pos += 1; FastTypeTag.Ref
            case _        => hints.tag
          }
        } else if (hints.isElidedType && primitives.contains(hints.tag.key)) {
          hints.tag
        } else {
          val lookahead = BinaryEncoder.decodeByteFrom(pos)
          lookahead match {
            case NULL_TAG =>
              pos += 1
              FastTypeTag.Null
            case ELIDED_TAG =>
              pos += 1
              hints.tag
            case REF_TAG =>
              pos += 1
              FastTypeTag.Ref
            case _ =>
              val (typeString, newpos) = BinaryEncoder.decodeStringFrom(pos)
              pos = newpos
              typeString
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

    def readPrimitive(): Any = {
      var newpos = pos
      val res = lastTagRead.key match {
          case KEY_NULL    => null
          case KEY_REF     => newpos = pos+4 ; lookupUnpicklee(BinaryEncoder.decodeIntFrom(pos))
          case KEY_BYTE    => newpos = pos+1 ; BinaryEncoder.decodeByteFrom(pos)
          case KEY_SHORT   => newpos = pos+2 ; BinaryEncoder.decodeShortFrom(pos)
          case KEY_CHAR    => newpos = pos+2 ; BinaryEncoder.decodeCharFrom(pos)
          case KEY_INT     => newpos = pos+4 ; BinaryEncoder.decodeIntFrom(pos)
          case KEY_LONG    => newpos = pos+8 ; BinaryEncoder.decodeLongFrom(pos)
          case KEY_BOOLEAN => newpos = pos+1 ; BinaryEncoder.decodeBooleanFrom(pos)
          case KEY_FLOAT   =>
            val r = BinaryEncoder.decodeIntFrom(pos)
            newpos = pos+4
            java.lang.Float.intBitsToFloat(r)
          case KEY_DOUBLE  =>
            val r = BinaryEncoder.decodeLongFrom(pos)
            newpos = pos+8
            java.lang.Double.longBitsToDouble(r)

          case KEY_SCALA_STRING | KEY_JAVA_STRING => val r = BinaryEncoder.decodeStringFrom(pos); newpos = r._2 ; r._1

          case KEY_ARRAY_BYTE => val r = BinaryEncoder.decodeByteArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_SHORT => val r = BinaryEncoder.decodeShortArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_CHAR => val r = BinaryEncoder.decodeCharArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_INT => val r = BinaryEncoder.decodeIntArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_LONG => val r = BinaryEncoder.decodeLongArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_BOOLEAN => val r = BinaryEncoder.decodeBooleanArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_FLOAT => val r = BinaryEncoder.decodeFloatArrayFrom(pos); newpos = r._2 ; r._1
          case KEY_ARRAY_DOUBLE => val r = BinaryEncoder.decodeDoubleArrayFrom(pos); newpos = r._2 ; r._1
      }

      pos = newpos
      res
    }

    def atObject: Boolean = !atPrimitive

    def readField(name: String): BinaryPickleReader =
      this

    def endEntry(): Unit = { /* do nothing */ }

    def beginCollection(): PReader = this

    def readLength(): Int = {
      val length = BinaryEncoder.decodeIntFrom(pos)
      pos += 4
      length
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
    def createReader(pickle: PickleType, mirror: Mirror) = new BinaryPickleReader(pickle.value, mirror, this)
  }
}
