package de.bst.five_cities.solutions

import de.bst.five_cities.opengeodb.{GeoObject, GeoDistance}
import org.apache.spark.rdd.RDD

object FiveCitiesSparkTutorialSolutions {
  def solutionFiveCities(cities: RDD[GeoObject]): RDD[List[GeoDistance]] =
    (cities cartesian cities
      filter (p => p._1 != p._2)
      map (p => p._1 distanceTo p._2)
      groupBy (_.a)).values map (_.toList.sorted take 5)

  def einwohnerDichte(filter: RDD[GeoObject]): Seq[(String, Double)] =
    (filter map (g => g.getName -> g.getEinwohner / g.getFlaeche)).collect

  /*
  def solutionFiveVillages(cities: RDD[GeoObject]): RDD[List[GeoDistance]] = {
    val radius = 20.0
    val regionCenters: List[GeoObject] = (cities coalesce 1 mapPartitions (x => regionalCoverage(x, radius))).collect.toList
    println(s"Calculation was parallelized in ${regionCenters.size} regions.")
    val villageClusters = cities

    val villageClusters = cities.flatMapToPair(geo -> region2city(regionCenters, geo, radius * 2)).groupByKey.flatMap(center2geos -> geoDistances(center2geos._1, center2geos._2, radius)).mapToPair(cluster -> new Tuple2 <> (//
      cluster.isEmpty() ? "?": cluster.get (0).getA(), //
      cluster)).groupByKey.mapToPair(city2clusters -> new Tuple2 <>( //
      city2clusters._1, //
      city2clusters._2.iterator().next())).sortByKey(true, 1).values
    villageClusters
  }

  private def regionalCoverage(cities: Iterator[GeoObject], radius: Double): Iterator[GeoObject] = {
    cities.toList.sorted

    stream(spliteratorUnknownSize(cities, ORDERED), false).collect(() -> newArrayList(), (l, geo) -> {
      if (l.stream().allMatch(c -> GeoDistance.dist(c, geo) > radius))
        l.add(geo);
    }, (l1, l2) -> union(l1, l2))
  }

  private def region2city(regionCenters: Nothing, geo: Nothing, radius: Double): Nothing = {
    return regionCenters.stream.filter(c -> GeoDistance.dist(c, geo) < radius).map(c -> new Tuple2 <>(c, geo)).collect(toList)
  }

  private def geoDistances(center: Nothing, geosInRegion: Nothing, radius: Double): Nothing = {
    val geos: Nothing = newArrayList(geosInRegion)
    return geos.stream.filter(geoA -> geoA.dist(center) < radius).map(geoA -> {
      return geos.stream() //
        .filter(geoB -> !geoA.equals(geoB)) //
        .map(geoB -> new GeoDistance(geoA, geoB)) //
        .sorted() //
        .limit(5) //
        .collect(toList());
    }).collect(toList)
  }
  */
}
