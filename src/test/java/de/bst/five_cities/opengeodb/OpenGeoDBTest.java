package de.bst.five_cities.opengeodb;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class OpenGeoDBTest {

	@Test
	public void getTSV() throws Exception {
		File liechtenstein = OpenGeoDB.getTSVFile("LI");
		assertTrue(FileUtils.readFileToString(liechtenstein).contains("Nendeln"));
	}

	@Test
	public void getTSVColumnMapping() throws Exception {
		Map<String, Integer> mapping = OpenGeoDB.getTSVColumnMapping("LI");
		assertEquals((int) mapping.get("loc_id"), 0);
		assertEquals((int) mapping.get("ascii"), 2);
		assertEquals((int) mapping.get("lat"), 4);
	}

}
