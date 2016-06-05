package de.bst.five_cities.opengeodb;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import de.bst.five_cities.utils.DownloadUtils;

/**
 * @see http://opengeodb.org/wiki/OpenGeoDB
 *
 */
public class OpenGeoDB {

	public static final String DOWNLOAD_BASE_URL = "http://www.fa-technik.adfc.de/code/opengeodb";
	public static final String LIECHTENSTEIN_TSV = "LI.tab";
	private static File localDir = new File("tmp/opengeodb");

	public static File getTSVFile(String countryID) {
		try {
			return DownloadUtils.getFile(new URL(DOWNLOAD_BASE_URL + "/" + countryID + ".tab"), localDir);
		} catch (Exception e) {
			throw new RuntimeException("Error downloading country file for " + countryID, e);
		}
	}

	public static Map<String, Integer> getTSVColumnMapping(String countryID) {

		Map<String, Integer> result = new TreeMap<>();
		File tsvFile = getTSVFile(countryID);
		String firstLine = firstLine(tsvFile);
		if (firstLine.charAt(0) != '#')
			throw new NoSuchElementException("Missing Header line in " + tsvFile);

		String[] splitted = firstLine.substring(1).split("\t");
		for (int i = 0; i < splitted.length; i++)
			result.put(splitted[i], i);
		return result;

	}

	private static String firstLine(File tsv) {
		try {
			LineIterator lineIterator = FileUtils.lineIterator(tsv);
			String firstLine = lineIterator.next();
			lineIterator.close();
			return firstLine;
		} catch (IOException e) {
			throw new RuntimeException("Error reading country file for " + tsv, e);
		}
	}

}
