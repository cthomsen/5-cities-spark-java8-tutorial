package de.bst.five_cities.opengeodb;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.spark.broadcast.Broadcast;

public class GeoObject implements Serializable {

	private static final long serialVersionUID = -6810268884235023962L;

	private int id;

	private String[] splits;

	private Map<String, Integer> mapping;

	public GeoObject(String line, Map<String, Integer> mapping) {
		splits = line.split("\t");
		this.mapping = mapping;
		this.id = Integer.parseInt(splits[0]);
	}

	public GeoObject(String line, Broadcast<Map<String, Integer>> mapping) {
		this(line, mapping.value());
	}

	public GeoDistance distanceTo(GeoObject otherGeoObject) {
		return new GeoDistance(this, otherGeoObject);
	}

	public double dist(GeoObject other) {
		return GeoDistance.dist(this, other);
	}

	public void writeGeoEntry(PrintStream out) {
		for (Entry<String, Integer> key2index : mapping.entrySet()) {
			Integer index = key2index.getValue();
			if (index < splits.length)
				out.println("\t" + key2index.getKey() + ": " + splits[index]);
		}
		out.println();
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return getString("name");
	}

	public String getString(int index) {
		return index < splits.length ? splits[index] : null;
	}

	public String getString(String key) {
		return getString(index(key));
	}

	private int index(String key) {
		Integer index = mapping.get(key);
		if (index != null)
			return index;
		else
			throw new NoSuchElementException("No mapping for " + key);
	}

	public double getEinwohner() {
		String key = "einwohner";
		return parseDouble(key);
	}

	public double getFlaeche() {
		String key = "flaeche";
		return parseDouble(key);
	}

	public boolean hasPosition() {
		return getString("lat") != null && getString("lon") != null //
				&& !getString("lat").isEmpty() && !getString("lon").isEmpty() //
				&& Double.isFinite(getLatitude()) && Double.isFinite(getLongtitude());
	}

	public double getLatitude() {
		return Double.parseDouble(getString("lat"));
	}

	public double getLongtitude() {
		return Double.parseDouble(getString("lon"));
	}

	public int getLevel() {
		return parseInt("level");
	}

	private long parseLong(String key) {
		String asString = getString(key);
		return asString.isEmpty() ? 0 : Long.parseLong(asString);
	}

	private double parseDouble(String key) {
		String asString = getString(key);
		return asString.isEmpty() ? 0.0 : Double.parseDouble(asString);
	}

	private int parseInt(String key) {
		String asString = getString(key);
		return asString == null || asString.isEmpty() ? 0 : Integer.parseInt(asString);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("GeoObject [" + getId() + "/" + getString("name"));
		Arrays.asList("einwohner", "lat", "lon", "level", "typ").stream().forEach(key -> {
			sb.append(" ");
			sb.append(key);
			sb.append("=");
			sb.append(getString(key));
		});
		sb.append("]");
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		GeoObject other = (GeoObject) obj;
		if (id != other.id)
			return false;
		return true;
	}

}
