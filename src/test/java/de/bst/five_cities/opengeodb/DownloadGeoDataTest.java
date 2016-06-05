package de.bst.five_cities.opengeodb;

import static java.util.Arrays.*;
import static org.apache.commons.io.FileUtils.*;
import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import de.bst.five_cities.opengeodb.OpenGeoDB;
import de.bst.five_cities.utils.DownloadUtils;

public class DownloadGeoDataTest {

	private File targetDir;
	private URL sourceURL;

	@Before
	public void setup() throws Exception {
		targetDir = new File("./tmp/download-test");
		sourceURL = new URL(OpenGeoDB.DOWNLOAD_BASE_URL + "/" + OpenGeoDB.LIECHTENSTEIN_TSV);

		deleteDirectory(targetDir);
	}

	@Test
	public void downloadedContent() throws Exception {

		File downloaded = DownloadUtils.getFile(sourceURL, targetDir);

		String content = FileUtils.readFileToString(downloaded);
		asList("Triesenberg", "Gaflei", "Nendeln").stream().forEach( //
				ort -> assertTrue(content.contains(ort)));
	}

	@Test
	public void willNotDownloadIfFileIsAlreadyThere() throws Exception {

		File firstDownload = DownloadUtils.getFile(sourceURL, targetDir);
		firstDownload.setLastModified(1234567000L);

		File secondDownload = DownloadUtils.getFile(sourceURL, targetDir);

		assertEquals("no second download should happen", 1234567000L, secondDownload.lastModified());
	}

}
