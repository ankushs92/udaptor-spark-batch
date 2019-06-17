package rdfs

class TripleReader (triples: List[Tuple3[String, String, String]]) {
  val tuples: List[(String, String)] = triples.map(t => Tuple2(t._1, t._3))
}
