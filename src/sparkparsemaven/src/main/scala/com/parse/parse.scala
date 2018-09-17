package com.parse

import java.io.{File, FileWriter, IOException, StringReader}
import gov.uspto.patent.PatentDocFormatDetect
import gov.uspto.common.filter.FileFilterChain
import gov.uspto.common.filter.SuffixFilter
import gov.uspto.patent.bulk.DumpFileXml
import gov.uspto.patent.serialize.JsonMapper
import gov.uspto.patent.PatentReader
//import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object parse {

  def parse(xml_file: String) {
    val inputFile = new File(xml_file)
    val limit = 1
    val writeFile = true

    val patentDocFormat = new PatentDocFormatDetect().fromFileName(inputFile)
    val json = new JsonMapper(true, false)
    val dumpReader = new DumpFileXml(inputFile)
    val filters = new FileFilterChain()
    //filters.addRule(new PathFileFilter(""));
    filters.addRule(new SuffixFilter("xml"))
    dumpReader.setFileFilter(filters)
    dumpReader.open()

    var i = 1
    while (dumpReader.hasNext && i <= limit) {
      val xmlDocStr = dumpReader.next
      try {
        val patentReader = new PatentReader(patentDocFormat)
        try {
          val xmlDocReader = new StringReader(xmlDocStr)
          val patent = patentReader.read(xmlDocReader)
          val patentId = patent.getDocumentId.toText
          System.out.println(patentId)
          //System.out.println("Patent Object: " + patent.toString());

          val writer = new FileWriter(patentId + ".json")
          try {
            json.write(patent, writer)
            if (!writeFile) System.out.println("JSON: " + writer.toString)
          } catch {
            case e: IOException =>
              System.err.println("Failed to write file for: " + patentId + "\n" + e.getStackTrace)
          } finally writer.close()
        } finally System.out.println("Finished to converting: " + xml_file + "\n")
      }

      {
        i += 1
      }
    }
  }


  def main(args: Array[String]) {

    val test_xml_file = "/Users/wilson/Insight/Projects/Patents_BROKEN/data/prelim/ipgb20160119_wk03/ipgb20160119.xml"
    parse(test_xml_file)
  }
}

