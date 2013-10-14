package scala.pickling.binaryfileio

import org.scalatest.FunSuite
import java.io.File

import scala.io.Source
import scala.pickling._
import scala.pickling.internal._
import scala.pickling.binary._
import scala.pickling.io.BinaryFileEncodedOutput

case class Person(name: String, age: Int, car: Option[String], child: Person)

class FileIOTest extends FunSuite {
  test("simple") {
	val pp = Person("Junior", 12, None, null)
	val p = Person("James", 45, Some("Ford"), pp)

	// RAM
	val pickle1 = p.pickle
	val unpickle1 = pickle1.unpickle[Person]

	// FILE
	val tmpFile = File.createTempFile("pickling", "fileoutput")
	p.pickleTo(new BinaryFileEncodedOutput(tmpFile))
	
	val pickle2 = new FilePickle(tmpFile)
	val unpickle2 = pickle2.unpickle[Person]


    assert(unpickle1 == unpickle2)
  }
}