package de.bst.five_cities.opengeodb

import java.io.File
import java.net.URL

import de.bst.five_cities.utils.DownloadUtils

import scala.io.Source

/**
  * @see http://opengeodb.org/wiki/OpenGeoDB
  */
object OpenGeoDB {
  private val DOWNLOAD_BASE_URL = "http://www.fa-technik.adfc.de/code/opengeodb"
  private val LOCAL_DIR = new File("tmp/opengeodb")

  def getTSVFile(countryID: String): File =
    DownloadUtils getFile(new URL(s"$DOWNLOAD_BASE_URL/$countryID.tab"), LOCAL_DIR)

  def getTSVColumnMapping(countryID: String): Map[String, Int] = {
    val tsvFile: File = getTSVFile(countryID)
    val firstLine = getFirstLine(tsvFile)
    if (!firstLine.startsWith("#")) throw new NoSuchElementException(s"Missing Header line in $tsvFile")
    (firstLine substring 1 split "\t").view.zipWithIndex.toMap
  }

  private def getFirstLine(tsv: File): String = (Source fromFile tsv).getLines.next
}
