package de.bst.five_cities.utils

import java.io.{FileWriter, File}
import java.net.URL
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils.getName
import org.apache.commons.io.IOUtils.copy

object DownloadUtils {
  def getFile(source: URL, targetDir: File): File = {
    val targetFile = new File(targetDir, getName(source.getPath))
    if (targetFile.exists) {
      notifySkippingOfDownload(targetFile)
      targetFile
    } else downloadFile(source, targetFile)
  }

  private def notifySkippingOfDownload(targetFile: File) {
    val lastModified = new Date(targetFile.lastModified)
    val now = new Date
    val secondsOld = (now.getTime - lastModified.getTime) / 1000.0
    println(s"Skipping download. File ${targetFile.getName} already exits and is $secondsOld seconds old $lastModified")
  }

  private def downloadFile(source: URL, targetFile: File): File = {
    FileUtils forceMkdir targetFile.getParentFile
    val input = source.openStream
    val output = new FileWriter(targetFile)
    copy(input, output)
    input.close
    output.close
    targetFile
  }
}

