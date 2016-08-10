package de.bst.five_cities.opengeodb

import java.io.PrintStream

import org.apache.spark.broadcast.Broadcast

@SerialVersionUID(-6810268884235023962L)
class GeoObject(val id: Int, val splits: Seq[String], val mapping: Map[String, Int]) extends Serializable {
  val PRIME = 31

  def distanceTo(otherGeoObject: GeoObject): GeoDistance = GeoDistance(this, otherGeoObject)

  def dist(other: GeoObject): Double = GeoDistance.dist(this, other)

  def writeGeoEntry(out: PrintStream) = mapping foreach {
    entry =>
      val index: Integer = entry._2
      if (entry._2 < splits.length) out.println(s"\t${entry._1}: ${splits(entry._2)}")
  }

  def getName: String = getString("name")

  def getEinwohner: Double = parseDouble("einwohner")

  def getFlaeche: Double = parseDouble("flaeche")

  def hasPosition: Boolean =
    !getString("lat").isEmpty && !getString("lon").isEmpty && !getLatitude.isInfinite && !getLongtitude.isInfinite

  def getLatitude: Double = getString("lat").toDouble

  def getLongtitude: Double = getString("lon").toDouble

  def getLevel: Int = parseInt("level")

  override def toString: String = {
    val children = List("einwohner", "lat", "lon", "level", "typ") map (f => s"$f=${getString(f)}") mkString " "
    s"GeoObject [$id/${getString("name")} $children]"
  }

  override def hashCode: Int = PRIME + id

  private def getString(index: Int): String = if (index < splits.length) splits(index) else ""

  private def getString(key: String): String = getString(index(key))

  private def index(key: String): Int = {
    val index = mapping get key
    if (index.isDefined) index.get
    else throw new NoSuchElementException(s"No mapping for $key")
  }

  private def parseLong(key: String) = {
    val asString = getString(key)
    if (asString.isEmpty) 0l
    else asString.toLong
  }

  private def parseDouble(key: String) = {
    val asString = getString(key)
    if (asString.isEmpty) .0
    else asString.toDouble
  }

  private def parseInt(key: String) = {
    val asString = getString(key)
    if (asString.isEmpty) 0
    else asString.toInt
  }
}

object GeoObject {
  def apply(line: String, mapping: Map[String, Int]): GeoObject = {
    val splits = line split "\t"
    new GeoObject(splits(0).toInt, splits, mapping)
  }

  def apply(line: String, mapping: Broadcast[Map[String, Int]]): GeoObject = apply(line, mapping.value)
}
