// # 5 Cities - Eine Mini-Tutorial zu Spark mit Scala
//
// Spark als Big-Data-Toolset ist in aller Munde.
//
// Mit Spark kann man große Datenmengen, die auf viele
// Server verteilt sind parallel verarbeiten, und das
// geht (fast) so einfach, als würde man auf einer
// lokalen Collection arbeiten.
//
// Man kann es mit Scala, Python oder Java verwenden.
// Hier eine kleine Intro für Scala-Entwickler,
// die Scala-Collections und funktionen höherer Ordnung kennen,
// aber noch nie mit Spark gearbeitet haben.
//
// !HIDE
package de.bst.five_cities

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS

import de.bst.five_cities.solutions.FiveCitiesSparkTutorialSolutions
import org.apache.commons.io.FileUtils.deleteDirectory
import de.bst.five_cities.opengeodb.{GeoDistance, OpenGeoDB, GeoObject}
import de.bst.five_cities.utils.SparkUtils
import de.bst.five_cities.utils.SparkUtils.getSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}

import scala.collection.mutable
import scala.io.Source

//
// Das Projekt könnt ihr als Maven-Projekt direkt in Eure Entwicklungsumgebung laden,
// in IntelliJ beispielsweise mit *"File | Open"*.
//
// Damit ihr sofort herumprobieren könnt habe ich alle Beispiele
// und Übungen als einzeln ausführbare Tests in eine Unit-Test-Klasse
// `FiveCitiesSparkTutorialTest` verpackt.
//
class FiveCitiesSparkTutorialTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  //
  // !HIDE
  override def afterAll = SparkUtils.close

  //
  // ## Der `SparkContext` und ein *RDD*
  //
  // Der Einstiegspunkt in Spark ist der `SparkContext`.
  // Wenn Ihr wissen wollt, wie man einen erstellt,
  // schaut in den Code der Methode `getSparkContext()`.
  // Für unsere Tests arbeitet Spark im lokalen Modus,
  // d. h. alle benötigten Dienste werden in der selben
  // JVM gestartet.
  //
  // Eine zu verarbeitende Datenmenge in Spark nennt man *RDD*.
  // Spark kann Daten aus den verschiedensten Quellen lesen,
  // z. B. aus hdfs-Dateien in Hadoop, aus lokalen Dateien oder aus
  // relationalen Datenbanken.
  // Für Testzwecke kann man auch eine Scala-`List` als Datenquelle
  // heranziehen.
  // Die Methode `parallelize()` macht daraus ein *RDD* -> `sc parallelize List(1, 2, 3)`
  private def sampleRDD = getSparkContext parallelize List(3, 10, 20, 9)

  "Five Cities Spark Tutorial" should "create an RDD" in {
    sampleRDD should not be null
  }

  // ## Actions
  //
  // Sogenannte *Actions*, verarbeiten die Daten des RDD *und* liefern
  // ein Ergebnis an den Client. Ein paar Beispiele:
  // `count()` zählt die Anzahl Zeilen,
  // `foreach(..)` führt für jede Zeile einen Befehl aus (lokal im Client),
  // `reduce(..)` aggregiert die Daten, nach einer gegebenen Funktion.
  //
  it should "count RDDs" in {
    sampleRDD.count shouldBe 4
  }

  it should "run foreach action" in {
    sampleRDD foreach (i => println(s"Result contains $i"))
  }

  it should "run reduce action" in {
    sampleRDD reduce (_ + _) shouldBe 42
  }

  // ## Wohin mit den Ergebnissen?
  //
  // Wenn die Ergebnismenge nicht zu gross ist, kann man
  // sie sich als lokale Scala-`List` mittels `collect()` geben lassen.
  // Bei grösseren Datenmengen kann man sich mit `takeSample(..)` eine
  // Stichprobe geben lassen, oder das Ergebnis
  // mit `saveAsTextFile()` in Dateien schreiben lassen.
  //
  // **Achtung:** In der Regel schreibt spark die Ergebnisse parallel
  // in mehrere Dateien. Im Beispiel haben wir die Parallelität
  // durch `coalesce(1)` reduziert, um nur eine Datei zu bekommen.
  //
  it should "copy the results of a RDD to a collection" in {
    sampleRDD.collect should contain theSameElementsAs List(3, 10, 20, 9)
  }

  it should "write to file" in {
    val targetDir = "tmp/zahlen"
    deleteDirectory(new File(targetDir))
    sampleRDD coalesce 1 saveAsTextFile targetDir
    (Source fromFile s"$targetDir/part-00000").getLines.toList should have size (4)
  }

  // ## Dateien lesen
  //
  // Mit `textFile(..)` können wir Dateien lesen.
  //
  // Für die nachfolgenden Beispiele nutzen wir ein paar Tab-separierte
  // Dateien mit Geo-Daten der OpenGeoDB, in der Städte und Gemeinden mitsamt
  // Koordinaten und ein paar statistischen Daten eingetragen sind.
  //
  it should "read a file" in {
    (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath collect) mkString "\n" should include ("Triesenberg")
  }

  // ## Persistierung von Zwischenergebnissen
  //
  // Führt man zwei Aktionen auf demselben *RDD* aus,
  // dann liest Spark bei der Zweiten Aktion die Datenquelle erneut!
  // Mit `cache()` kann man Spark anweisen, sich das Zwischenergebnis zu
  // merken.
  //
  // > **Tipp** Unter http://localhost:4040 zeigt Spark ein UI, in dem
  // > man sehen kann, welche Jobs ausgeführt wurden. Wenn ihr in
  // > `afterTest()` einen Breakpoint setzt, wird das UI nicht sofort beendet,
  // > und Ihr könnt Euch in Ruhe anschauen, was Spark alles protokolliert
  // hat. Mehr dazu unten im Abschnitt über das Debugging.
  //
  // http://localhost:4040/storage/ zeigt, welche Zwischenergebnisse
  // gespeichert sind.
  //
  it should "use the Spark cache" in {
    val rdd = (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath cache)
    val allLines = rdd.count
    val activeLines = (rdd filter (!_.startsWith("#")) count)
    println(s"allLines = $allLines")
    println(s"activeLines = $activeLines")
    allLines should be > activeLines
    activeLines should not be 0
  }

  // Für die folgenden Tests stellen wir eine Hilfsmethode bereit
  private[five_cities] def getGeoObjectsRDD(countryId: String, mode: Int): RDD[GeoObject] =
  // um die Geo-Daten einzulesen und zu cachen.
  // !HIDE
    geoObjectsRDDcache get countryId getOrElse {
      val columnMappingsBroadcast = getSparkContext broadcast (OpenGeoDB getTSVColumnMapping countryId)
      val geoRDD = ((getSparkContext textFile (OpenGeoDB getTSVFile countryId getPath))
        filter (!_.startsWith("#"))
        map (GeoObject(_, columnMappingsBroadcast)))
      val cachedRDD = (if (mode == MODE_ALL) geoRDD else geoRDD filter (_.hasPosition)).cache
      geoObjectsRDDcache += countryId -> cachedRDD
      cachedRDD
    }

  var geoObjectsRDDcache = mutable.HashMap[String, RDD[GeoObject]]()

  val MODE_ALL = 0
  val MODE_WITH_POSITION = 1

  // ## Transformationen
  //
  // Zur Verarbeitung von RDD's kennt Spark neben *Aktionen*
  // auch *Transformationen*.
  //
  // Transformationen werden in den Spark-Workern ausgeführt,
  // und anders als *Aktionen* übertragen sie kein Ergebnis
  // zum Client, sondern liefern wieder einen RDD.
  // Auf diesen kann man, wenn man möchte weitere Transformationen.
  //
  // **Achtung!** Die Transformationen werden von Spark zunächst nur
  // gesammelt und kommen erst zur Ausführung, wenn eine *Aktion*
  // auf dem RDD ausgeführt wird.
  //
  // Eine sehr häufig verwendete Transformation ist `map(..):
  //
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

  // Sortierung ist eine weitere nützliche Transformation
  it should "sort a RDD" in {
    val sortedList = (getGeoObjectsRDD("LI", MODE_WITH_POSITION) sortBy(_.getEinwohner, false, 1) collect)
    println("Liechtenstein wohlsortiert:")
    sortedList foreach (x => println(x.getName))
    sortedList.head.getName shouldBe "Schaan"
  }

  // ## Aufgabe - Bundesländer
  //
  // Einwohner pro Quadratkilometer berechnen.
  it should "show the German Bundesländer" in {
    val filter = getGeoObjectsRDD("DE", MODE_ALL) filter (_.getLevel == 3)
    val einwohnerDichte = FiveCitiesSparkTutorialSolutions einwohnerDichte filter
    println("Bundesländer:")
    einwohnerDichte foreach (p => println(f"  ${p._1}, ${p._2}%3.1f Einw./km2"))
  }

  // ## Aufgabe - Five Cities
  // Betrachte Städte mit mehr als 100.000 Einwohnern.
  // Finde zu allen Städten, die jeweils 5 am nächsten gelegenen.
  it should "calculate the five nearest cities for every city with more than 100000 inhabitants" in {
    val cities = (getGeoObjectsRDD("DE", MODE_WITH_POSITION) filter (g => g.getLevel == 6 && g.getEinwohner > 100000) cache)
    startRecordingStatistics(cities)
    val result = (FiveCitiesSparkTutorialSolutions solutionFiveCities cities collect)
    endRecordingStatistics
    result foreach (println)
    result filter (_.head.a == "Hamburg") flatMap (_ map (_.b)) should contain theSameElementsAs List(
      "Lübeck", "Kiel", "Bremerhaven", "Bremen", "Oldenburg in Oldenburg")
  }

  it should "calculate the five nearest cities for every city with more than 5000 inhabitants" in {
    val cities = (getGeoObjectsRDD("DE", MODE_WITH_POSITION) filter (g => g.getLevel == 6 && g.getEinwohner > 5000) cache)
    startRecordingStatistics(cities)
    val result = (FiveCitiesSparkTutorialSolutions solutionFiveCities cities collect)
    endRecordingStatistics
    result foreach (println)
  }

  //
  // !HIDE
  private[five_cities] var start: Instant = null
  private[five_cities] var end: Instant = null
  private[five_cities] var distanceCalculationsStart: Long = 0
  private[five_cities] var distanceCalculationsEnd: Long = 0
  private[five_cities] var rddProcessed: RDD[_] = null

  def startRecordingStatistics(rdd: RDD[_]) {
    distanceCalculationsStart = GeoDistance.countDistancesCalculations.get
    rddProcessed = rdd
    start = Instant.now
  }

  def endRecordingStatistics {
    distanceCalculationsEnd = GeoDistance.countDistancesCalculations.get
    end = Instant.now
    println(s"Seconds processing: ${start until (end, SECONDS)}")
    println(s"Number of input rows: ${rddProcessed.count}")
    println(s"Number of distance calculations: ${distanceCalculationsEnd - distanceCalculationsStart}")
  }

  /*
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
  */
}
