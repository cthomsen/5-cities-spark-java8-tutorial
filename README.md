# 5 Cities - Eine Mini-Tutorial zu Spark mit Java 8

Spark als Big-Data-Toolset ist in aller Munde.

Mit Spark kann man große Datenmengen, die auf viele
Server verteilt sind parallel verarbeiten, und das
geht (fast) so einfach, als würde man auf einer 
lokalen Collection arbeiten.

Man kann es mit Scala, Python oder Java verwenden.
Hier eine kleine Intro für Java-Entwickler,
die Java8-Streams kennen, aber noch nie mit Spark gearbeitet haben.

Falls ihr noch nie mit Java8-Stream gearbeitet habt: Es lohnt sich!
In der Klasse `Java8StreamSamplesTest` findet Ihr ein kleines Beispiel
als Appetitanreger.


Das Projekt könnt ihr als Maven-Projekt direkt in Eure Entwicklungsumgebung laden,
in Eclipse beispielsweise mit *"File | Import ... | Existing Maven Projects"*.

Damit ihr sofort herumprobieren könnt habe ich alle Beispiele
und Übungen als einzeln ausführbare Tests in eine Unit-Test-Klasse
`FiveCitiesSparkTutorialTest` verpackt.

```java
public class FiveCitiesSparkTutorialTest extends AbstractTest {

```

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
```java
	@Test
	public void createAnRDD() {
		List<Integer> numbersAsJavaList = asList(3, 10, 20, 9);

		JavaRDD<Integer> numbersAsRDD = getSparkContext().parallelize(numbersAsJavaList);

		assertNotNull("expecting an RDD with 4 rows", numbersAsRDD);
	}

```
## Actions

Sogenannte *Actions*, verarbeiten die Daten des RDD *und* liefern
ein Ergebnis an den Client. Ein paar Beispiele:
`count()` zählt die Anzahl Zeilen,
`foreach(..)` führt für jede Zeile einen Befehl aus (lokal im Client),
`reduce(..)` aggregiert die Daten, nach einer gegebenen Funktion.

```java
	@Test
	public void count() {
		JavaRDD<Integer> numbersAsRDD = getSparkContext().parallelize(asList(3, 10, 20, 9));

		long rowCount = numbersAsRDD.count();

		assertEquals("expecting an RDD with 4 rows", 4, rowCount);
	}

	@Test
	public void foreachAction() {
		JavaRDD<Integer> numbers = getSparkContext().parallelize(asList(3, 10, 20, 9));

		numbers.foreach(i -> out.println("result countains " + i));
	}

	@Test
	public void reduceAction() {
		JavaRDD<Integer> numbersAsRDD = getSparkContext().parallelize(asList(3, 10, 20, 9));

		int theAnswer = numbersAsRDD.reduce((a, b) -> (a + b));

		assertEquals(42, theAnswer);
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

```java
	@Test
	public void copyingTheResultsToAJavaCollection() {
		JavaRDD<Integer> numbers = getSparkContext().parallelize(asList(3, 10, 20, 9));

		List<Integer> javaCollection = numbers.collect();

		assertEquals(new HashSet<>(asList(3, 9, 10, 20)), new HashSet<>(javaCollection));
	}

	@Test
	public void writingToFiles() throws Exception {
		deleteDirectory(new File("tmp/zahlen"));
		JavaRDD<Integer> numbers = getSparkContext().parallelize(asList(3, 10, 20, 9));
		numbers.coalesce(1);

		numbers.saveAsTextFile("tmp/zahlen");

		assertEquals(4, FileUtils.readLines(new File("tmp/zahlen/part-00000")).size());
	}

```
## Dateien lesen

Mit `textFile(..)` können wir Dateien lesen.

Für die nachfolgenden Beispiele nutzen wir ein paar Tab-separierte
Dateien mit Geo-Daten der OpenGeoDB, in der Städte und Gemeinden mitsamt
Koordinaten und ein paar statistischen Daten eingetragen sind.

```java
	@Test
	public void readingAFile() {
		File file = OpenGeoDB.getTSVFile("LI");

		List<String> rdd = getSparkContext() //
				.textFile(file.getPath()) //
				.collect();

		assertTrue(join(rdd, "\n").contains("Triesenberg"));
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

```java
	@Test
	public void usingCache() {

		JavaRDD<String> liechtensteinRDD = getSparkContext() //
				.textFile(OpenGeoDB.getTSVFile("LI").getPath()) //
				.cache();

		long allLines = liechtensteinRDD.count();
		long activeLines = liechtensteinRDD //
				.filter(line -> !line.startsWith("#")) //
				.count();

		out.printf("allLines = %d\n", allLines);
		out.printf("activeLines = %d\n", activeLines);

		assertTrue(allLines > activeLines);
		assertTrue(activeLines > 0);
	}

```
Für die folgenden Tests stellen wir eine Hilfsmethode bereit
```java
	JavaRDD<GeoObject> getGeoObjectsRDD(String countryId, Mode mode)
```
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

```java
	@Test
	public void transformToObjects() {
		JavaRDD<GeoObject> geoObjectsRDD = getGeoObjectsRDD("LI", WITH_POSITION);

		JavaRDD<String> namesRDD = geoObjectsRDD.map(geo -> geo.getName());

		List<String> names = namesRDD.collect();
		out.println("Names in Liechtenstein: " + names);
		assertTrue(names.contains("Balzers"));
		assertTrue(names.contains("Schaan"));
	}

	@Test
	public void countByValue() {

		getGeoObjectsRDD("LI", WITH_POSITION) //
				.map(geo -> geo.getLevel()) //
				.countByValue() //
				.forEach((level, count) -> out.printf("%s -> %s\n", level, count));

	}

	@Test
	public void countByValueDE() {

		getGeoObjectsRDD("DE", WITH_POSITION) //
				.map(geo -> geo.getLevel()) //
				.countByValue() //
				.forEach((level, count) -> out.printf("%s -> %s\n", level, count));
	}

```
Sortierung ist eine weitere nützliche Transformation
```java
	@Test
	public void sort() {

		List<GeoObject> geoObjectsLI = getGeoObjectsRDD("LI", WITH_POSITION) //
				.sortBy(geo -> geo.getEinwohner(), false, 1) //
				.collect();

		out.println("Liechtenstein wohlsortiert:");
		geoObjectsLI.forEach(geo -> out.println("\t" + geo.getName()));

		assertEquals("Schaan", geoObjectsLI.get(0).getName());
	}

```
## Aufgabe - Bundesländer

Einwohner pro Quadratkilometer berechnen.
```java
	@Test
	public void bundeslaender() {
		JavaRDD<GeoObject> filter = getGeoObjectsRDD("DE", ALL) //
				.filter(geo -> geo.getLevel() == 3);

```
Kommentiere die nachfolgende Zeile aus und ersetze sie durch Deine
Lösung.
```java
		List<Tuple2<String, Double>> einwohnerDichte = FiveCitiesSparkTutorialSolutions.einwohnerDichte(filter);

		System.out.println("Bundesländer");
		einwohnerDichte.forEach(p -> out.printf("  %s, %3.1f Einw./km2\n", p._1, p._2));
		System.out.println("OK");
	}

```
## Aufgabe - Five Cities
Betrachte Städte mit mehr als 100.000 Einwohnern.
Finde zu allen Städten, die jeweils 5 am nächsten gelegenen.
```java
	@Test
	public void fiveCities() throws IOException {
		JavaRDD<GeoObject> cities = getGeoObjectsRDD("DE", WITH_POSITION) //
				.filter(geo -> geo.getLevel() == 6 && geo.getEinwohner() > 100000) //
				.cache();
		startRecordingStatistics(cities);
		JavaRDD<List<GeoDistance>> cityClusters;

```
Kommentiere die nachfolgende Zeile aus und ersetze sie durch Deine
Lösung.
```java
		cityClusters = FiveCitiesSparkTutorialSolutions.solutionFiveCities(cities);

		List<List<GeoDistance>> result = cityClusters.collect();
		endRecordingStatistics();
		printStatistics();
		result.forEach(distances -> out.println(distances));
		List<String> actualHamburgCluster = result.stream() //
				.filter(cluster -> cluster.get(0).getA().equals("Hamburg"))//
				.findAny().get().stream() //
				.map(dist -> dist.getB()) //
				.collect(toList());
		assertEquals( //
				asList("Lübeck", "Kiel", "Bremerhaven", "Bremen", "Oldenburg in Oldenburg"), //
				actualHamburgCluster);
	}

```
## Aufgabe - Five Villages

Betrachte alle Städe auf Geo-Level 6.
Finde zu allen Städten, die jeweils 5 am nächsten gelegenen in max. 20km
Entfernung.
```java
	@Test
	public void fiveVillages() throws IOException {
		JavaRDD<GeoObject> cities = getGeoObjectsRDD("DE", WITH_POSITION) //
				.filter(geo -> geo.getLevel() == 6) //
				.cache();
		String targetDir = "tmp/cities";
		deleteDirectory(new File(targetDir));
		startRecordingStatistics(cities);

```
Hier kommt Deine Lösung!
```java
		JavaRDD<List<GeoDistance>> villageClusters = FiveCitiesSparkTutorialSolutions.solutionFiveVillages(cities);

		villageClusters.saveAsTextFile(targetDir);
		endRecordingStatistics();
		out.println();
		printStatistics();
		System.out.println("Results written to: " + targetDir);
	}

```

## Tipp: Arbeiten in der Debug-Umgebung

Setze einen Breakpoint in `afterTest()` vor dem `return`,
dann wird die Umgebung nicht sofort heruntergefahren,
und das Spark-UI (unter http://localhost:4040) bleibt offen.

Wenn der Debugger am Breakpoint oben hält,
markiere einfach den Namen einer Methode, z. B. `foreachAction()` und
drücke cmd-u
(bzw. "Ausführen" oder "Execute" im Kontextmenü), um einen
den Code auszuführen, ohne die laufende JVM zu verlassen.

```java

	@After
	public void afterTest() throws IOException {
		out.println("test performed.");
		return;
	}

```
### Viel Spaß mit Spark!
