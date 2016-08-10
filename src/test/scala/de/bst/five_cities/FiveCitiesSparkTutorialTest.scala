// # 5 Cities - Eine Mini-Tutorial zu Spark mit Java 8
//
// Spark als Big-Data-Toolset ist in aller Munde.
//
// Mit Spark kann man große Datenmengen, die auf viele
// Server verteilt sind parallel verarbeiten, und das
// geht (fast) so einfach, als würde man auf einer
// lokalen Collection arbeiten.
//
// Man kann es mit Scala, Python oder Java verwenden.
// Hier eine kleine Intro für Java-Entwickler,
// die Java8-Streams kennen, aber noch nie mit Spark gearbeitet haben.
//
// Falls ihr noch nie mit Java8-Stream gearbeitet habt: Es lohnt sich!
// In der Klasse `Java8StreamSamplesTest` findet Ihr ein kleines Beispiel
// als Appetitanreger.
//
// !HIDE
package de.bst.five_cities

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import de.bst.five_cities.opengeodb.{OpenGeoDB, GeoObject}
import de.bst.five_cities.utils.SparkUtils
import de.bst.five_cities.utils.SparkUtils.getSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}

import scala.collection.mutable
import scala.io.Source

class FiveCitiesSparkTutorialTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val MODE_ALL = 0
  val MODE_WITH_POSITION = 1

  override def afterAll = SparkUtils.close

  var geoObjectsRDDcache = mutable.HashMap[String, RDD[GeoObject]]()

  private[five_cities] def getGeoObjectsRDD(countryId: String, mode: Int): RDD[GeoObject] =
    geoObjectsRDDcache get countryId getOrElse {
      val columnMappingsBroadcast = getSparkContext broadcast (OpenGeoDB getTSVColumnMapping countryId)
      val geoRDD = ((getSparkContext textFile (OpenGeoDB getTSVFile countryId getPath))
        filter (!_.startsWith("#"))
        map (GeoObject(_, columnMappingsBroadcast)))
      val cachedRDD = (if (mode == MODE_ALL) geoRDD else geoRDD filter (_.hasPosition)).cache
      geoObjectsRDDcache += countryId -> cachedRDD
      cachedRDD
    }

  private def sampleRDD = getSparkContext parallelize List(3, 10, 20, 9)

  "Five Cities Spark Tutorial" should "create an RDD" in {
    sampleRDD should not be null
  }

  it should "count RDDs" in {
    sampleRDD.count shouldBe 4
  }

  it should "run foreach action" in {
    sampleRDD foreach (i => println(s"Result contains $i"))
  }

  it should "run reduce action" in {
    sampleRDD reduce (_ + _) shouldBe 42
  }

  it should "copy the results of a RDD to a collection" in {
    sampleRDD.collect should contain theSameElementsAs List(3, 10, 20, 9)
  }

  it should "write to file" in {
    val targetDir = "tmp/zahlen"
    deleteDirectory(new File(targetDir))
    sampleRDD coalesce 1 saveAsTextFile targetDir
    (Source fromFile s"$targetDir/part-00000").getLines.toList should have size (4)
  }

  it should "read a file" in {
    (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath collect) mkString "\n" should include ("Triesenberg")
  }

  it should "use the Spark cache" in {
    val rdd = (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath cache)
    val allLines = rdd.count
    val activeLines = (rdd filter (!_.startsWith("#")) count)
    println(s"allLines = $allLines")
    println(s"activeLines = $activeLines")
    allLines should be > activeLines
    activeLines should not be 0
  }

  it should "transform lines to objects" in {
    val names = (getGeoObjectsRDD("LI", MODE_WITH_POSITION) map (_.getName) collect)
    println("Names in Liechtenstein:")
    names foreach (println)
    names should (contain("Balzers") and contain("Schaan"))
  }

  it should "count by value in LI" in {
    (getGeoObjectsRDD("LI", MODE_WITH_POSITION) map (_.getLevel) countByValue) foreach (x => println(s"${x._1} -> ${x._2}"))
  }

  it should "count by value in DE" in {
    (getGeoObjectsRDD("DE", MODE_WITH_POSITION) map (_.getLevel) countByValue) foreach (x => println(s"${x._1} -> ${x._2}"))
  }

  it should "sort a RDD" in {
    val sortedList = (getGeoObjectsRDD("LI", MODE_WITH_POSITION) sortBy(_.getEinwohner, false, 1) collect)
    println("Liechtenstein wohlsortiert:")
    sortedList foreach (x => println(x.getName))
    sortedList.head.getName shouldBe "Schaan"
  }

  /*
  it should "show the German Bundesländer" in {
    getGeoObjectsRDD("DE", ALL) filter (_.getLevel == 3)
  }

  @Test def bundeslaender {
    val filter: JavaRDD[GeoObject] = getGeoObjectsRDD("DE", ALL).filter(geo -> geo.getLevel() == 3)
    val einwohnerDichte: util.List[(String, Double)] = FiveCitiesSparkTutorialSolutions.einwohnerDichte(filter)
    System.out.println("Bundesländer")
    einwohnerDichte.forEach(p -> out.printf("  %s, %3.1f Einw./km2\n", p._1, p._2))
    System.out.println("OK")
  }

  @Test
  def fiveCities {
    val cities: JavaRDD[GeoObject] = getGeoObjectsRDD("DE", WITH_POSITION).filter(geo -> geo.getLevel() == 6 && geo.getEinwohner() > 100000).cache
    startRecordingStatistics(cities)
    var cityClusters: JavaRDD[util.List[GeoDistance]] = null
    cityClusters = FiveCitiesSparkTutorialSolutions.solutionFiveCities(cities)
    val result: util.List[util.List[GeoDistance]] = cityClusters.collect
    endRecordingStatistics
    printStatistics
    result.forEach(distances -> out.println(distances))
    val actualHamburgCluster: util.List[String] = result.stream.filter(cluster -> cluster.get(0).getA().equals("Hamburg")).findAny.get.stream.map(dist -> dist.getB()).collect(toList)
    assertEquals(asList("Lübeck", "Kiel", "Bremerhaven", "Bremen", "Oldenburg in Oldenburg"), actualHamburgCluster)
  }

  @Test
  def fiveVillages {
    val cities: JavaRDD[GeoObject] = getGeoObjectsRDD("DE", WITH_POSITION).filter(geo -> geo.getLevel() == 6).cache
    val targetDir: String = "tmp/cities"
    deleteDirectory(new File(targetDir))
    startRecordingStatistics(cities)
    val villageClusters: JavaRDD[util.List[GeoDistance]] = FiveCitiesSparkTutorialSolutions.solutionFiveVillages(cities)
    villageClusters.saveAsTextFile(targetDir)
    endRecordingStatistics
    out.println
    printStatistics
    System.out.println("Results written to: " + targetDir)
  }

  private[five_cities] var start: Instant = null
  private[five_cities] var end: Instant = null
  private[five_cities] var distanceCalculationsStart: Long = 0
  private[five_cities] var distanceCalculationsEnd: Long = 0
  private[five_cities] var rddProcessed: JavaRDD[_] = null

  def startRecordingStatistics(rdd: JavaRDD[_]) {
    distanceCalculationsStart = GeoDistance.countDistancesCalculations.get
    rddProcessed = rdd
    start = Instant.now
  }

  def endRecordingStatistics {
    distanceCalculationsEnd = GeoDistance.countDistancesCalculations.get
    end = Instant.now
  }

  def printStatistics {
    out.println("Seconds processing: " + start.until(end, SECONDS))
    out.println("Number of input rows: " + rddProcessed.count)
    out.println("Number of distance calculations: " + (distanceCalculationsEnd - distanceCalculationsStart))
  }
  */
}
