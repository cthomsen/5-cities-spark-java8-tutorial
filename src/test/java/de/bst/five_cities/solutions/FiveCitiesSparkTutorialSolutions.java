package de.bst.five_cities.solutions;

import static com.google.common.collect.Lists.*;
import static java.lang.System.*;
import static java.util.Spliterator.*;
import static java.util.Spliterators.*;
import static java.util.stream.Collectors.*;
import static java.util.stream.StreamSupport.*;
import static org.apache.commons.collections.ListUtils.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaRDD;

import de.bst.five_cities.opengeodb.GeoDistance;
import de.bst.five_cities.opengeodb.GeoObject;
import scala.Tuple2;

public class FiveCitiesSparkTutorialSolutions {

	public static JavaRDD<List<GeoDistance>> solutionFiveCities(JavaRDD<GeoObject> cities) {
		JavaRDD<List<GeoDistance>> cityClusters;
		cityClusters = cities.cartesian(cities) //
				.filter(geoPair -> !geoPair._1.equals(geoPair._2)) //
				.map(geoPair -> geoPair._1.distanceTo(geoPair._2)) //
				.groupBy(dist -> dist.getA()) //
				.values().map(geos -> sortAndLimit(geos, 5)); //
		return cityClusters;
	}

	private static List<GeoDistance> sortAndLimit(Iterable<GeoDistance> distances, int limit) {
		// Und hier mal wieder java 8.
		return StreamSupport.stream(distances.spliterator(), true) //
				.sorted() //
				.limit(limit) //
				.collect(toList());
	}

	public static JavaRDD<List<GeoDistance>> solutionFiveVillages(JavaRDD<GeoObject> cities) {
		double radius = 20.0;
		List<GeoObject> regionCenters = cities //
				.coalesce(1) //
				.mapPartitions(geos -> regionalCoverage(geos, radius)) //
				.collect();
		out.println("Calculation was parallelized in " + regionCenters.size() + " regions.");

		JavaRDD<List<GeoDistance>> villageClusters = cities //
				.flatMapToPair(geo -> region2city(regionCenters, geo, radius * 2)) //
				.groupByKey() //
				.flatMap(center2geos -> geoDistances(center2geos._1, center2geos._2, radius)) //
				.mapToPair(cluster -> new Tuple2<>(//
						cluster.isEmpty() ? "?" : cluster.get(0).getA(), //
						cluster)) //
				.groupByKey()
				.mapToPair(city2clusters -> new Tuple2<>( //
						city2clusters._1, //
						city2clusters._2.iterator().next())) //
				.sortByKey(true, 1) //
				.values();

		return villageClusters;
	}

	private static List<GeoObject> regionalCoverage(Iterator<GeoObject> cities, double radius) {
		return stream(spliteratorUnknownSize(cities, ORDERED), false) //
				.collect(//
						() -> newArrayList(), //
						(l, geo) -> {
							if (l.stream().allMatch(c -> GeoDistance.dist(c, geo) > radius))
								l.add(geo);
						} , //
						(l1, l2) -> union(l1, l2));
	}

	private static List<Tuple2<GeoObject, GeoObject>> region2city(List<GeoObject> regionCenters, GeoObject geo,
			double radius) {
		return regionCenters.stream() //
				.filter(c -> GeoDistance.dist(c, geo) < radius)//
				.map(c -> new Tuple2<>(c, geo))//
				.collect(toList());
	}

	private static List<List<GeoDistance>> geoDistances(GeoObject center, Iterable<GeoObject> geosInRegion,
			double radius) {
		ArrayList<GeoObject> geos = newArrayList(geosInRegion);
		return geos.stream() //
				.filter(geoA -> geoA.dist(center) < radius) //
				.map(geoA -> {
					return geos.stream() //
							.filter(geoB -> !geoA.equals(geoB)) //
							.map(geoB -> new GeoDistance(geoA, geoB)) //
							.sorted() //
							.limit(5) //
							.collect(toList());
				}) //
				.collect(toList());
	}

	public static List<Tuple2<String, Double>> einwohnerDichte(JavaRDD<GeoObject> filter) {
		List<Tuple2<String, Double>> einwohnerDichte = filter //
				.mapToPair(geo -> new Tuple2<>(//
						geo.getName(), //
						(double) geo.getEinwohner() / geo.getFlaeche())) //
				.collect();
		return einwohnerDichte;
	}

}
