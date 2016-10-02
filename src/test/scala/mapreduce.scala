package pariterator

import org.scalatest.FunSpec
import org.scalatest.PrivateMethodTester
import org.scalatest.Matchers
import org.scalatest.prop.Checkers
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Arbitrary, Gen}
import scala.util._

class MapReduceSpec
    extends FunSpec
    with Matchers
    with PrivateMethodTester
    with Checkers {

  object MR extends MapReduceScheme[Int, Int] {
    val map = (i: Int) => i
    val reduce = (i: Int, x: Int) => i + x
  }

  describe("trivi") {
    it("empty") {
      MapReduceTraversal
        .traverse[Int, Int](List[Int](), MR)
        .toString should equal(Failure(
        new java.lang.IllegalArgumentException("Empty iterator")).toString)
    }
    it("1, 1 map 1 reduce") {
      (MapReduceTraversal.traverse[Int, Int](List(1), MR, 1, 1)) should equal(
        Success(1))
    }
    it("1,2,3 1 map 1 reduce") {
      (MapReduceTraversal
        .traverse[Int, Int](List(1, 2, 3), MR, 1, 1)) should equal(Success(6))
    }
    it("1, 2 map 2 reduce") {
      (MapReduceTraversal.traverse[Int, Int](List(1), MR, 2, 2)) should equal(
        Success(1))
    }
    it("1-10, 1 map 1 reduce") {
      (MapReduceTraversal.traverse[Int, Int](
        List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        MR,
        1,
        1)) should equal(Success(55))
    }

    it("stress slow reduce ") {
      object MR2 extends MapReduceScheme[Int, Int] {
        val map = (i: Int) => i
        val reduce = (i: Int, x: Int) => { Thread.sleep(100); i + x }
      }
      val n = 1000
      (MapReduceTraversal
        .traverse[Int, Int](1 to n toList, MR2, 1, 1)) should equal(
        Success(n * (n + 1) / 2))
    }
  }

}
