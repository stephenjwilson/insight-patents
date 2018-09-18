package com.parse

import java.io.{File, FileWriter, IOException, StringReader}
import java.util

import com.amazonaws.services.s3.AmazonS3
import gov.uspto.patent.PatentDocFormatDetect
import gov.uspto.patent.serialize.JsonMapper
import gov.uspto.patent.PatentReader
import gov.uspto.patent.model.{Citation, CitationType, NplCitation, PatCitation}
import javax.json.JsonArrayBuilder
import gov.uspto.patent.bulk.DumpFileAps
import gov.uspto.patent.bulk.DumpFileXml
import javax.json.Json
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import java.io.FileOutputStream
import java.util.zip.ZipInputStream
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

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
  def parse(xml_file: String): String = {
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
    val file_name = inputFile + ".json"
    file_name
  }

  def pull_from_s3(s3: AmazonS3, bucket_name: String, file_name: String): String = {

    val fullObject = s3.getObject(bucket_name, file_name)

    val buffer = new Array[Byte](1024)
    val s3is = fullObject.getObjectContent
    //get the zip file content
    val zis = new ZipInputStream(s3is)
    //get the zipped file list entry
    val ze = zis.getNextEntry

    val fileName = ze.getName
    val newFile = new File(fileName)

    System.out.println("file unzip : " + newFile.getAbsoluteFile)

    val fos = new FileOutputStream(newFile)

    var len: Int = zis.read(buffer)

    while (len > 0) {
      fos.write(buffer, 0, len)
      len = zis.read(buffer)
    }

    fos.close()
    newFile.getAbsoluteFile.toString
  }

  def push_to_s3(s3: AmazonS3, bucket_name: String, file: String, key: String): Unit = {
    s3.putObject(bucket_name, key, file)
  }

  def process_s3_key(key: String): Unit = {
    val s3: AmazonS3 = AmazonS3ClientBuilder.defaultClient
    val bucket_name = "patent-xml-zipped"
    val file_name = pull_from_s3(s3, bucket_name, key)
    val json_file = parse(file_name)
    push_to_s3(s3, bucket_name, json_file, key + ".json")
  }

  def main(args: Array[String]) {
    val bucket_name = "patent-xml-zipped"
    val s3: AmazonS3 = AmazonS3ClientBuilder.defaultClient
    val result = s3.listObjectsV2(bucket_name)
    val objects = result.getObjectSummaries

    val conf = new SparkConf().setAppName("XML-JSON").setMaster("local")
    val sc = new SparkContext(conf)

    val files = sc.parallelize(objects.asScala.filterNot(_.getKey contains "json").map(x => x.getKey))
    println(files.getClass)
    //    files.map((j:String) => process_s3_key(bucket_name,j))
    files.map(process_s3_key)


    //    for( os <- objects.asScala.filterNot(_.getKey contains "json")) {
    //      process_s3_key(s3, bucket_name, os.getKey)
    //      System.out.println("Processed: " + os.getKey)
    //    }

  }
}

