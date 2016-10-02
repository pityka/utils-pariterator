package pariterator

import scala.collection.generic.CanBuildFrom
import scala.collection.GenTraversableOnce
import scala.util.Try

trait MapReduceScheme[A, I] {
  val map: A => I
  val reduce: (I, I) => I
}

object MapReduceScheme {

  def combine[A, I1, I2](
      mr1: MapReduceScheme[A, I1],
      mr2: MapReduceScheme[A, I2]): MapReduceScheme[A, (I1, I2)] =
    new MapReduceScheme[A, (I1, I2)] {
      val map = (a: A) => (mr1.map(a), mr2.map(a))
      val reduce: ((I1, I2), (I1, I2)) => (I1, I2) = (i1: (I1, I2),
                                                      i2: (I1, I2)) =>
        (mr1.reduce(i1._1, i2._1), mr2.reduce(i1._2, i2._2))
    }

  def combine[A, I1, I2, I3](
      mr1: MapReduceScheme[A, I1],
      mr2: MapReduceScheme[A, I2],
      mr3: MapReduceScheme[A, I3]
  ): MapReduceScheme[A, (I1, I2, I3)] = new MapReduceScheme[A, (I1, I2, I3)] {
    val map = (a: A) => (mr1.map(a), mr2.map(a), mr3.map(a))
    val reduce = (i1: (I1, I2, I3), i2: (I1, I2, I3)) =>
      (mr1.reduce(i1._1, i2._1),
       mr2.reduce(i1._2, i2._2),
       mr3.reduce(i1._3, i2._3))
  }

  def combine[A, I1, I2, I3, I4](
      mr1: MapReduceScheme[A, I1],
      mr2: MapReduceScheme[A, I2],
      mr3: MapReduceScheme[A, I3],
      mr4: MapReduceScheme[A, I4]
  ): MapReduceScheme[A, (I1, I2, I3, I4)] =
    new MapReduceScheme[A, (I1, I2, I3, I4)] {
      val map = (a: A) => (mr1.map(a), mr2.map(a), mr3.map(a), mr4.map(a))
      val reduce = (i1: (I1, I2, I3, I4), i2: (I1, I2, I3, I4)) =>
        (mr1.reduce(i1._1, i2._1),
         mr2.reduce(i1._2, i2._2),
         mr3.reduce(i1._3, i2._3),
         mr4.reduce(i1._4, i2._4))
    }

  def combine[A](mrs: Map[String, MapReduceScheme[A, Any]])
    : MapReduceScheme[A, Map[String, Any]] =
    new MapReduceScheme[A, Map[String, Any]] {
      val map = (a: A) => mrs.map(x => x._1 -> x._2.map(a))
      val reduce = (i1: Map[String, Any], i2: Map[String, Any]) =>
        mrs.map(x => x._1 -> x._2.reduce(i1(x._1), i2(x._1)))
    }

}

object MapReduceTraversal {

  def traverse[A, I](
      collection: GenTraversableOnce[A],
      mapreduce: MapReduceScheme[A, I],
      numberOfReducers: Int = 10,
      numberOfMappers: Int = 10
  ): Try[I] = {
    val mapped = map(collection, numberOfMappers, false)(mapreduce.map)
    scala.util.Try(reduce(mapped, numberOfReducers)(mapreduce.reduce))
  }

  def traverse[A, I](
      coll: GenTraversableOnce[A],
      concurrency: Int
  )(map1: A => I)(reduce1: (I, I) => I): Try[I] =
    traverse(coll, new MapReduceScheme[A, I] {
      val map = map1
      val reduce = reduce1
    }, concurrency, concurrency)

}
