package de.bst.five_cities.utils

import java.io.PrintWriter

import scala.io.Source

object CreateDoc {
  def main(args: Array[String]) {
    generateDoc("src/test/scala/de/bst/five_cities/FiveCitiesSparkTutorialTest.scala", "README.md")
  }

  def generateDoc(source: String, target: String) {
    val lines = (Source fromFile source).getLines map (BlockOfLines(_))
    val document = ((List[BlockOfLines]() /: lines)
      ((l, b) => if(l.isEmpty || b.blockType != l.last.blockType) l :+ b else l.init :+ (l.last merge b))
      map (_.format) mkString "\n")
    new PrintWriter("README.md") {
      write(document)
      close
    }
  }

  private object BlockOfLines {
    val TYPE_CODE = "CODE"
    val TYPE_TEXT = "TEXT"
    private val COMMENT_PATTERN = "^\\s*// ?(.*)$".r
    private val HIDE_PATTERN = "^\\s*//\\s*!HIDE\\s*$".r

    def apply(line: String): BlockOfLines =
      new BlockOfLines(line :: Nil, typeOf(line), HIDE_PATTERN findFirstIn line isDefined)

    def typeOf(line: String): String =
      if (HIDE_PATTERN findFirstIn line isDefined) TYPE_CODE
      else if (COMMENT_PATTERN findFirstIn line isDefined) TYPE_TEXT
      else TYPE_CODE
  }

  private class BlockOfLines(var lines: List[String], val blockType: String, var hide: Boolean) {
    def merge(other: BlockOfLines) = {
      lines = lines ::: other.lines
      this
    }

    override def toString: String = s"${blockType.toString}\n\t${lines mkString "\n\t"}"

    def format: String = {
      import BlockOfLines._
      blockType match {
        case TYPE_CODE => if (!hide) s"```scala\n${lines mkString "\n"}```" else ""
        case TYPE_TEXT => lines map {
          l =>
            val matcher = COMMENT_PATTERN findFirstMatchIn l
            if (matcher.isDefined) matcher.get group 1 else l
        } mkString "\n"
      }
    }
  }
}

