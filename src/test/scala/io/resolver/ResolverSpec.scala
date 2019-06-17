package io.resolver

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResolverSpec extends org.specs2.mutable.Specification {

  "Resolver" >> {

    "Should convert a simple structure to json" >> {

      val path = List("_pπ", "name")
      val values = List("Joe", "James", "Haley")

      val output = "{\"p\":[{\"name\":\"Joe\"},{\"name\":\"James\"},{\"name\":\"Haley\"}]}"

      val resultJSON = Resolver.resolveJson(path.asJava, values.asJava)

      resultJSON must beEqualTo(output)

    }


    "Should convert another simple structure to json" >> {

      val path = List("_p", "nameπ")
      val values = List("Joe", "James", "Haley")

      val output = "{\"p\":{\"name\":[\"Joe\",\"James\",\"Haley\"]}}"

      val resultJSON = Resolver.resolveJson(path.asJava, values.asJava)

      resultJSON must beEqualTo(output)

    }

    "Should convert nested structure to json" >> {

      val path = List("_a", "_b", "_c", "_d", "_e", "_f", "_g", "_h")
      val values = List("i")

      val output = "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h\":\"i\"}}}}}}}}"

      val resultJSON = Resolver.resolveJson(path.asJava, values.asJava)

      resultJSON must beEqualTo(output)

    }

    "Should Merge two Json" >> {

      val json1 = """{"playlist":{"nameOfTheArtist":"Florence + The Machine"}}"""
      val json2 = """{"playlist":{"nameOfTheArtist":"James Arthur"}}"""

      val mapper = new ObjectMapper()
      val node = mapper.readTree(json1)
      val node2 = mapper.readTree(json2)

      val merge = Resolver.merge(node, node2)

      val str = mapper.writeValueAsString(merge)

      str must beEqualTo("""{"playlist":[{"nameOfTheArtist":"Florence + The Machine"}, {"nameOfTheArtist":"James Arthur"}]}""")



    }

  }

}
