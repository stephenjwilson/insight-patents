package samples

import java.io.File

import org.junit._
import Assert._

import scala.io._
import net.liftweb.json.parse
import org.json4s.JObject
import scala.collection.immutable.HashMap

/*
Tests to check that parsing the USTPO XML is working and creating json files with the right fields
 */
@Test
class JSONTest {

  @Test
  def test_json(): Unit = {
    val json_files = new File("./").listFiles.
      filter { f => f.isFile && f.getName.endsWith(".json") }.
      map(_.getAbsolutePath).toList
    for (json_file <- json_files) {
      val json = Source.fromFile(json_file)
      // parse
      val parsed_json = parse(json.mkString)
      // Make sure that certain parameters are present
      val hash_json = parsed_json.values.asInstanceOf[HashMap.HashTrieMap[String, JObject]]
      assert(hash_json.get("applicationId").isDefined)
      assert(hash_json.get("publishedDate").isDefined)
      assert(hash_json.get("citations").isDefined)
    }

  }

}


