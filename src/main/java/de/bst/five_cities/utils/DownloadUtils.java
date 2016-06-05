package de.bst.five_cities.utils;

import static org.apache.commons.io.FilenameUtils.*;
import static org.apache.commons.io.IOUtils.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;

import org.apache.commons.io.FileUtils;

public class DownloadUtils {

	public static File getFile(URL source, File targetDir) throws IOException {
		File targetFile = new File(targetDir, getName(source.getPath()));
		if (targetFile.exists()) {
			notifySkippingOfDownload(targetFile);
			return targetFile;
		}

		return downloadFile(source, targetFile);
	}

	private static void notifySkippingOfDownload(File targetFile) {
		Date lastModified = new Date(targetFile.lastModified());
		Date now = new Date();
		double secondsOld = (now.getTime() - lastModified.getTime()) / 1000.0;
		System.out.println("Skipping download. File " + targetFile + " already exits and is " + secondsOld
				+ " seconds old (lastModified = " + lastModified + ")");
	}

	private static File downloadFile(URL source, File targetFile) throws IOException {
		FileUtils.forceMkdir(targetFile.getParentFile());
		InputStream input = source.openStream();
		FileWriter output = new FileWriter(targetFile);
		copy(input, output);
		input.close();
		output.close();

		return targetFile;
	}

}
