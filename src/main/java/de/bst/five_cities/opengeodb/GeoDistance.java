package de.bst.five_cities.opengeodb;

import static java.lang.Math.*;
import static java.lang.String.*;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class GeoDistance implements Comparable<GeoDistance>, Serializable {

	private static final long serialVersionUID = -7050866324523179632L;

	private String a;
	private String b;

	private double dist;

	public GeoDistance(GeoObject cityA, GeoObject cityB) {
		this.a = cityA.getName();
		this.b = cityB.getName();
		this.dist = dist(cityA, cityB);
	}

	public String getA() {
		return a;
	}

	public double getDist() {
		return dist;
	}

	public String getB() {
		return b;
	}

	@Override
	public int compareTo(GeoDistance o) {
		int c = a.compareTo(o.a);
		return c == 0 ? Double.compare(dist, o.dist) : c;
	}

	@Override
	public String toString() {
		return a + format("-%1.1f-", dist) + b;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((a == null) ? 0 : a.hashCode());
		result = prime * result + ((b == null) ? 0 : b.hashCode());
		long temp;
		temp = Double.doubleToLongBits(dist);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GeoDistance other = (GeoDistance) obj;
		if (a == null) {
			if (other.a != null)
				return false;
		} else if (!a.equals(other.a))
			return false;
		if (b == null) {
			if (other.b != null)
				return false;
		} else if (!b.equals(other.b))
			return false;
		if (Double.doubleToLongBits(dist) != Double.doubleToLongBits(other.dist))
			return false;
		return true;
	}

	public static AtomicLong countDistancesCalculations = new AtomicLong(0);

	public static double dist(GeoObject a, GeoObject b) {
		countDistancesCalculations.incrementAndGet();

		double a_lat = toRadians(a.getLatitude());
		double a_lon = toRadians(a.getLongtitude());
		double b_lat = toRadians(b.getLatitude());
		double b_lon = toRadians(b.getLongtitude());
		double earthDiameterKm = 6380;

		return earthDiameterKm * //
				acos(sin(b_lat) * sin(a_lat) + cos(b_lat) * cos(a_lat) * cos(b_lon - a_lon));
	}

}