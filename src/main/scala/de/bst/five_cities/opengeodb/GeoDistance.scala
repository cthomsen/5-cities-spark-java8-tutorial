package de.bst.five_cities.opengeodb

import java.lang.Double.doubleToLongBits
import java.lang.Math._
import java.util.concurrent.atomic.AtomicLong

@SerialVersionUID(-7050866324523179632L)
class GeoDistance(val a: String, val b: String, val dist: Double = .0) extends Comparable[GeoDistance] with Serializable {
  val PRIME = 31

  def compareTo(o: GeoDistance): Int = a compareTo o.a match {
    case 0 => (dist - o.dist).toInt
    case x => x
  }

  override def toString: String = f"$a-$dist%1.1f-$b"

  override def hashCode: Int = {
    var result = 1
    result = PRIME * result + (if (a == null) 0 else a.hashCode)
    result = PRIME * result + (if (b == null) 0 else b.hashCode)
    val temp = doubleToLongBits(dist)
    PRIME * result + (temp ^ (temp >>> 32)).toInt
  }

  /*
  override def equals(obj: AnyRef): Boolean = {
    if (this eq obj) return true
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val other: GeoDistance = obj.asInstanceOf[GeoDistance]
    if (a == null) {
      if (other.a != null) return false
    }
    else if (!(a == other.a)) return false
    if (b == null) {
      if (other.b != null) return false
    }
    else if (!(b == other.b)) return false
    if (doubleToLongBits(dist) != doubleToLongBits(other.dist)) return false
    return true
  }
  */
}

object GeoDistance {
  val EARTH_DIAMETER_KM = 6380.0
  val countDistancesCalculations = new AtomicLong(0)

  def apply(cityA: GeoObject, cityB: GeoObject) =
    new GeoDistance(cityA.getName, cityB.getName, GeoDistance dist(cityA, cityB))

  def dist(a: GeoObject, b: GeoObject): Double = {
    countDistancesCalculations.incrementAndGet
    val a_lat = toRadians(a.getLatitude)
    val a_lon = toRadians(a.getLongtitude)
    val b_lat = toRadians(b.getLatitude)
    val b_lon = toRadians(b.getLongtitude)
    EARTH_DIAMETER_KM * acos(sin(b_lat) * sin(a_lat) + cos(b_lat) * cos(a_lat) * cos(b_lon - a_lon))
  }
}