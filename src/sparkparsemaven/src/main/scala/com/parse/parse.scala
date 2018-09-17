package com.parse

import java.io.{File, FileWriter, IOException, StringReader}
import java.util

import gov.uspto.patent.PatentDocFormatDetect
import gov.uspto.patent.serialize.JsonMapper
import gov.uspto.patent.PatentReader
import gov.uspto.patent.model.{Citation, CitationType, NplCitation, PatCitation}
import javax.json.JsonArrayBuilder
import gov.uspto.patent.bulk.DumpFileAps
import gov.uspto.patent.bulk.DumpFileXml
import javax.json.Json
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  *
  *         parser for USTPO XML files
  */

object parse {
  /**
    * mapCitations accepts a list of citations and formats it to json
    *
    * @param CitationList
    * @return
    */
  def mapCitations(CitationList: util.Collection[Citation]): JsonArrayBuilder = {
    val arBldr = Json.createArrayBuilder
    val var3 = CitationList.iterator

    while (var3.hasNext) {
      val cite = var3.next
      if (cite.getCitType eq CitationType.NPLCIT) {
        val nplCite = cite.asInstanceOf[NplCitation]
        val nplObj = Json.createObjectBuilder.add("num", nplCite.getNum).add("type", "NPL").add("citedBy", nplCite.getCitType.toString).add("examinerCited", nplCite.isExaminerCited).add("text", nplCite.getCiteText)
        val extractedObj = Json.createObjectBuilder.add("quotedText", nplCite.getQuotedText).add("patentId", if (nplCite.getPatentId != null) nplCite.getPatentId.toText
        else "")
        nplObj.add("extracted", extractedObj)
        arBldr.add(nplObj)
      }
      else if (cite.getCitType eq CitationType.PATCIT) {
        val patCite = cite.asInstanceOf[PatCitation]
        arBldr.add(Json.createObjectBuilder.add("num", patCite.getNum).add("type", "PATENT").add("citedBy", patCite.getCitType.toString).add("examinerCited", patCite.isExaminerCited).add("text", patCite.getDocumentId.toText))
      }
    }
    arBldr
  }

  /**
    * parse takes an xml format and converts it to json locally
    *
    * @param xml_file
    */
  def parse(xml_file: String) {
    val inputFile = new File(xml_file)

    val writeFile = true

    val patentDocFormat = new PatentDocFormatDetect().fromFileName(inputFile)
    val json = new JsonMapper(true, false)
    val Pattern = "([A-Z][a-z]+)".r
    val patent_format = Pattern.findFirstIn(patentDocFormat.toString)
    val dumpReader = if (patent_format.toString == "Redbook") {
      new DumpFileAps(inputFile)
    }
    else {
      new DumpFileXml(inputFile)
    }
    dumpReader.open()

    val builder = Json.createObjectBuilder()
    while (dumpReader.hasNext) {
      val xmlDocStr = dumpReader.next
      try {
        val patentReader = new PatentReader(patentDocFormat)
        try {
          val xmlDocReader = new StringReader(xmlDocStr)
          try {
            val patent = patentReader.read(xmlDocReader)
            val patentId = patent.getDocumentId.toText
            System.out.println(patentId)
            builder.add(patentId, mapCitations(patent.getCitations()))

          } catch {
            case e: Exception => println(e) // TODO: ensure ''. error not a data issue
          }
        }
      }

    }
    val output = builder.build()
    val writer = new FileWriter(inputFile + ".json")
    writer.write(output.toString)
    writer.close()
  }

  def main(args: Array[String]) {

    val test_xml_file = "/Users/wilson/Insight/Projects/Patents_BROKEN/data/prelim/ipgb20160119_wk03/ipgb20160119.xml"
    parse(test_xml_file)
  }
}

