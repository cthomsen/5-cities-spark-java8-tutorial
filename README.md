# 5 Cities - Eine Mini-Tutorial zu Spark mit Java 8

Spark als Big-Data-Toolset ist in aller Munde.

Mit Spark kann man große Datenmengen, die auf viele
Server verteilt sind parallel verarbeiten, und das
geht (fast) so einfach, als würde man auf einer
lokalen Collection arbeiten.

Man kann es mit Scala, Python oder Java verwenden.
Hier eine kleine Intro für Scala-Entwickler,
die Java-Collections und funktionen höherer Ordnung kennen,
aber noch nie mit Spark gearbeitet haben.

Falls ihr noch nie mit Java8-Stream gearbeitet habt: Es lohnt sich!
In der Klasse `Java8StreamSamplesTest` findet Ihr ein kleines Beispiel
als Appetitanreger.



Das Projekt könnt ihr als Maven-Projekt direkt in Eure Entwicklungsumgebung laden,
in Eclipse beispielsweise mit *"File | Import ... | Existing Maven Projects"*.

Damit ihr sofort herumprobieren könnt habe ich alle Beispiele
und Übungen als einzeln ausführbare Tests in eine Unit-Test-Klasse
`FiveCitiesSparkTutorialTest` verpackt.

```scala
class FiveCitiesSparkTutorialTest extends FlatSpec with Matchers with BeforeAndAfterAll {```



## Der `SparkContext` und ein *RDD*

Der Einstiegspunkt in Spark ist der `SparkContext`.
Wenn Ihr wissen wollt, wie man einen erstellt,
schaut in den Code der Methode `getSparkContext()`.
Für unsere Tests arbeitet Spark im lokalen Modus,
d. h. alle benötigten Dienste werden in der selben
JVM gestartet.

Eine zu verarbeitende Datenmenge in Spark nennt man *RDD*.
Spark kann Daten aus den verschiedensten Quellen lesen,
z. B. aus hdfs-Dateien in Hadoop, aus lokalen Dateien oder aus
relationalen Datenbanken.
Für Testzwecke kann man auch eine Java-`List` als Datenquelle
heranziehen.
Die Methode `parallelize()` macht daraus ein *RDD*.
```scala
  "Five Cities Spark Tutorial" should "create an RDD" in {
    sampleRDD should not be null
  }
```
## Actions

Sogenannte *Actions*, verarbeiten die Daten des RDD *und* liefern
ein Ergebnis an den Client. Ein paar Beispiele:
`count()` zählt die Anzahl Zeilen,
`foreach(..)` führt für jede Zeile einen Befehl aus (lokal im Client),
`reduce(..)` aggregiert die Daten, nach einer gegebenen Funktion.

```scala
  it should "count RDDs" in {
    sampleRDD.count shouldBe 4
  }

  it should "run foreach action" in {
    sampleRDD foreach (i => println(s"Result contains $i"))
  }

  it should "run reduce action" in {
    sampleRDD reduce (_ + _) shouldBe 42
  }
```
## Wohin mit den Ergebnissen?

Wenn die Ergebnismenge nicht zu gross ist, kann man
sie sich als lokale Java-`List` mittels `collect()` geben lassen.
Bei grösseren Datenmengen kann man sich mit `takeSample(..)` eine
Stichprobe geben lassen, oder das Ergebnis
mit `saveAsTextFile()` in Dateien schreiben lassen.

**Achtung:** In der Regel schreibt spark die Ergebnisse parallel
in mehrere Dateien. Im Beispiel haben wir die Parallelität
durch `coalesce(1)` reduziert, um nur eine Datei zu bekommen.

```scala
  it should "copy the results of a RDD to a collection" in {
    sampleRDD.collect should contain theSameElementsAs List(3, 10, 20, 9)
  }

  it should "write to file" in {
    val targetDir = "tmp/zahlen"
    deleteDirectory(new File(targetDir))
    sampleRDD coalesce 1 saveAsTextFile targetDir
    (Source fromFile s"$targetDir/part-00000").getLines.toList should have size (4)
  }
```
## Dateien lesen

Mit `textFile(..)` können wir Dateien lesen.

Für die nachfolgenden Beispiele nutzen wir ein paar Tab-separierte
Dateien mit Geo-Daten der OpenGeoDB, in der Städte und Gemeinden mitsamt
Koordinaten und ein paar statistischen Daten eingetragen sind.

```scala
  it should "read a file" in {
    (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath collect) mkString "\n" should include ("Triesenberg")
  }
```
## Persistierung von Zwischenergebnissen

Führt man zwei Aktionen auf demselben *RDD* aus,
dann liest Spark bei der Zweiten Aktion die Datenquelle erneut!
Mit `cache()` kann man Spark anweisen, sich das Zwischenergebnis zu
merken.

> **Tipp** Unter http://localhost:4040 zeigt Spark ein UI, in dem
> man sehen kann, welche Jobs ausgeführt wurden. Wenn ihr in
> `afterTest()` einen Breakpoint setzt, wird das UI nicht sofort beendet,
> und Ihr könnt Euch in Ruhe anschauen, was Spark alles protokolliert
hat. Mehr dazu unten im Abschnitt über das Debugging.

http://localhost:4040/storage/ zeigt, welche Zwischenergebnisse
gespeichert sind.

```scala
  it should "use the Spark cache" in {
    val rdd = (getSparkContext textFile (OpenGeoDB getTSVFile "LI").getPath cache)
    val allLines = rdd.count
    val activeLines = (rdd filter (!_.startsWith("#")) count)
    println(s"allLines = $allLines")
    println(s"activeLines = $activeLines")
    allLines should be > activeLines
    activeLines should not be 0
  }
```
Für die folgenden Tests stellen wir eine Hilfsmethode bereit
```scala
  private[five_cities] def getGeoObjectsRDD(countryId: String, mode: Int): RDD[GeoObject] =```
um die Geo-Daten einzulesen und zu cachen.

## Transformationen

Zur Verarbeitung von RDD's kennt Spark neben *Aktionen*
auch *Transformationen*.

Transformationen werden in den Spark-Workern ausgeführt,
und anders als *Aktionen* übertragen sie kein Ergebnis
zum Client, sondern liefern wieder einen RDD.
Auf diesen kann man, wenn man möchte weitere Transformationen.

**Achtung!** Die Transformationen werden von Spark zunächst nur
gesammelt und kommen erst zur Ausführung, wenn eine *Aktion*
auf dem RDD ausgeführt wird.

Eine sehr häufig verwendete Transformation ist `map(..):

```scala
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
```
Sortierung ist eine weitere nützliche Transformation
```scala
  it should "sort a RDD" in {
    val sortedList = (getGeoObjectsRDD("LI", MODE_WITH_POSITION) sortBy(_.getEinwohner, false, 1) collect)
    println("Liechtenstein wohlsortiert:")
    sortedList foreach (x => println(x.getName))
    sortedList.head.getName shouldBe "Schaan"
  }
```
## Aufgabe - Bundesländer

Einwohner pro Quadratkilometer berechnen.
```scala
  it should "show the German Bundesländer" in {
    val filter = getGeoObjectsRDD("DE", MODE_ALL) filter (_.getLevel == 3)
    val einwohnerDichte = FiveCitiesSparkTutorialSolutions einwohnerDichte filter
    println("Bundesländer:")
    einwohnerDichte foreach (p => println(f"  ${p._1}, ${p._2}%3.1f Einw./km2"))
  }
```
## Aufgabe - Five Cities
Betrachte Städte mit mehr als 100.000 Einwohnern.
Finde zu allen Städten, die jeweils 5 am nächsten gelegenen.
```scala
  it should "calculate the five nearest cities for every city with more than 100000 inhabitants" in {
    val cities = (getGeoObjectsRDD("DE", MODE_WITH_POSITION) filter (g => g.getLevel == 6 && g.getEinwohner > 100000) cache)
    startRecordingStatistics(cities)
    val result = (FiveCitiesSparkTutorialSolutions solutionFiveCities cities collect)
    endRecordingStatistics
    result foreach (println)
    result filter (_.head.a == "Hamburg") flatMap (_ map (_.b)) should contain theSameElementsAs List(
      "Lübeck", "Kiel", "Bremerhaven", "Bremen", "Oldenburg in Oldenburg")
  }

  it should "calculate the five nearest cities for every city" in {
    val cities = (getGeoObjectsRDD("DE", MODE_WITH_POSITION) filter (g => g.getLevel == 6 && g.getEinwohner > 5000) cache)
    startRecordingStatistics(cities)
    val result = (FiveCitiesSparkTutorialSolutions solutionFiveCities cities collect)
    endRecordingStatistics
    result foreach (println)
  }
```

