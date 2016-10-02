package pariterator

import org.scalatest.FunSpec
import org.scalatest.Matchers
import collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global

class ParIteratorSpec extends FunSpec with Matchers {

  describe("test sequence reduce") {
    it("empty") {
      intercept[IllegalArgumentException](
        reduce(List[Int]().iterator, 1)(_ + _))
      intercept[IllegalArgumentException](
        reduce(List[Int]().iterator, 10)(_ + _))
    }
    it("1") {
      reduce(List[Int](1).iterator, 1)(_ + _) should equal(1)
      reduce(List[Int](1).iterator, 10)(_ + _) should equal(1)
    }
    it("2") {
      reduce(List[Int](1, 2).iterator, 1)(_ + _) should equal(3)
      reduce(List[Int](1, 2).iterator, 10)(_ + _) should equal(3)
    }
    it("3") {
      reduce(List[Int](1, 2, 3).iterator, 1)(_ + _) should equal(6)
      reduce(List[Int](1, 2, 3).iterator, 10)(_ + _) should equal(6)
    }
    it("100000k") {
      reduce(List[Int](1 to 100000: _*).iterator, 1)(_ + _) should equal(
        100000 * 100001 / 2)
      reduce(List[Int](1 to 100000: _*).iterator, 10)(_ + _) should equal(
        100000 * 100001 / 2)
    }
    it("20 slow") {
      val t1 = System.nanoTime
      reduce(List[Int](1 to 20: _*).iterator, 1) { (x, y) =>
        println(x + " " + y); Thread.sleep(1000); println(x + " " + y + "end");
        x + y
      } should equal(20 * 21 / 2)
      val t2 = System.nanoTime
      reduce(List[Int](1 to 20: _*).iterator, 10) { (x, y) =>
        println(x + " " + y); Thread.sleep(1000); println(x + " " + y + "end");
        x + y
      } should equal(20 * 21 / 2)
      val t3 = System.nanoTime
      (t3 - t2) should be < (t2 - t1)
    }

  }

  describe("test sequence") {
    it(" iterator with state 1 ") {
      var l = List[Int]()
      map(List[Int](1 to 1000: _*).iterator.filter { i =>
        l = i :: l
        true
      }, 1)(x => x).toList should equal(1 to 1000 toList)

      l.reverse should equal((1 to 1000).toList)

    }

    it(" iterator with state 10 ") {
      var l = List[Int]()
      map(List[Int](1 to 1000: _*).iterator.filter { i =>
        l = i :: l
        true
      }, 10)(x => x).toList should equal(1 to 1000 toList)

      l.reverse should equal((1 to 1000).toList)

    }

    it("empty") {
      map(List[Int]().iterator, 1)(x => x).toList should equal(Nil)
      map(List[Int]().iterator, 10)(x => x).toList should equal(Nil)
    }
    it("1") {
      map(List[Int](1).iterator, 1)(x => x.toString).toList should equal(
        List("1"))
      map(List[Int](1).iterator, 10)(x => x.toString).toList should equal(
        List("1"))
    }
    it("3") {
      map(List[Int](1, 2, 3).iterator, 1)(x => x.toString).toList should equal(
        List("1", "2", "3"))
      map(List[Int](1, 2, 3).iterator, 10)(x => x.toString).toList should equal(
        List("1", "2", "3"))
      map(List[Int](1, 2, 3).iterator, 2)(x => x.toString).toList should equal(
        List("1", "2", "3"))
    }
    it("10k") {
      map(List[Int](1 to 10000: _*).iterator, 1)(x => x.toString).toList should equal(
        List[Int](1 to 10000: _*).map(_.toString))
      map(List[Int](1 to 10000: _*).iterator, 10)(x => x.toString).toList should equal(
        List[Int](1 to 10000: _*).map(_.toString))
      map(List[Int](1 to 10000: _*).iterator, 2)(x => x.toString).toList should equal(
        List[Int](1 to 10000: _*).map(_.toString))
    }
    it("error") {
      intercept[RuntimeException](map(List[Int](1).iterator, 1)(x =>
        throw new RuntimeException("dsfd")).toList)
    }

  }

  describe("test sequence unordered") {
    it("empty") {
      map(List[Int]().iterator, 1, false)(x => x).toList should equal(Nil)
      map(List[Int]().iterator, 10, false)(x => x).toList should equal(Nil)
    }
    it("1") {
      map(List[Int](1).iterator, 1, false)(x => x.toString).toList should equal(
        List("1"))
      map(List[Int](1).iterator, 10, false)(x => x.toString).toList should equal(
        List("1"))
    }
    it("3") {
      map(List[Int](1, 2, 3).iterator, 1, false)(x => x.toString).toList should equal(
        List("1", "2", "3"))
      map(List[Int](1, 2, 3).iterator, 10, false)(x => x.toString).toList.sorted should equal(
        List("1", "2", "3").sorted)
      map(List[Int](1, 2, 3).iterator, 2, false)(x => x.toString).toList.sorted should equal(
        List("1", "2", "3").sorted)
    }
    it("10k") {
      map(List[Int](1 to 10000: _*).iterator, 1, false)(x => x.toString).toList.sorted should equal(
        List[Int](1 to 10000: _*).map(_.toString).sorted)
      map(List[Int](1 to 10000: _*).iterator, 10, false)(x => x.toString).toList.sorted should equal(
        List[Int](1 to 10000: _*).map(_.toString).sorted)
      map(List[Int](1 to 10000: _*).iterator, 10, false)(x => x.toString).toList shouldNot equal(
        List[Int](1 to 10000: _*).map(_.toString))
      map(List[Int](1 to 10000: _*).iterator, 2, false)(x => x.toString).toList.sorted should equal(
        List[Int](1 to 10000: _*).map(_.toString).sorted)
    }
    it("error") {
      intercept[RuntimeException](map(List[Int](1).iterator, 1, false)(x =>
        throw new RuntimeException("dsfd")).toList)
    }

  }
}
