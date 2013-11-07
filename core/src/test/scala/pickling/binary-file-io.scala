package scala.pickling.binaryfileio

import org.scalatest.FunSuite
import java.io._

import scala.pickling._
import scala.pickling.binary._

case class Person(name: String)
case class PersonNums(name: String, randNums: Array[Int]) {
  override def toString(): String = s"""Name: ${name} - Nums: ${randNums.toList}"""
  override def equals(obj: Any) = obj.isInstanceOf[PersonNums] && obj.asInstanceOf[PersonNums].name == name && randNums.toList == obj.asInstanceOf[PersonNums].randNums.toList
}

class BinaryFileIOTest extends FunSuite {
  test("simple") {
    val p = Person("James")

    val tmpFile = File.createTempFile("pickling", "fileoutput")
    val fileOut = new BinaryFileOutput(tmpFile)

    p.pickleTo(fileOut)
    fileOut.close()

    val bis = new BufferedInputStream(new FileInputStream(tmpFile))
    val fileContents = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toList
    bis.close()
    
    assert(fileContents == p.pickle.value.toList)
  }

  test("simple-w-collection") {
    val p = PersonNums("James", (1 to 200).toArray)

    val tmpFile = File.createTempFile("pickling", "fileoutput")
    val fileOut = new BinaryFileOutput(tmpFile)

    p.pickleTo(fileOut)
    fileOut.close()
    
    val bis = new BufferedInputStream(new FileInputStream(tmpFile))
    val fileContents = Stream.continually(bis.read).takeWhile(_ != -1).map(_.toByte).toList
    bis.close()
    
    assert(fileContents == p.pickle.value.toList)
  }
  
  test("write-n-read") {
    val p = PersonNums("James", (1 to 200).toArray)

    val tmpFile = File.createTempFile("pickling", "fileoutput")
    val fileOut = new BinaryFileOutput(tmpFile)

    p.pickleTo(fileOut)
    fileOut.close()

    val fileIn = new BinaryFileInput(tmpFile)
    val unpckl = fileIn.unpickle[PersonNums]
    fileIn.close()
    
    assert(unpckl == p)
  }
}
