package rdfs

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TripleReaderSpec extends org.specs2.mutable.Specification {

  "TripleReader" >> {

    "Read a list of triples" >> {
      val triples = List(Tuple3("something", "is", "somethingElse"), Tuple3("Music", "is", "Track"))
      val reader = new TripleReader(triples)

      reader.tuples must contain(exactly (
          Tuple2("something", "somethingElse"),
          Tuple2("Music", "Track")
      ))
    }

  }

}
