package io.resolver

import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object Resolver {

  def merge(mainNode: JsonNode, updateNode: JsonNode): JsonNode = {

    if (mainNode == null || updateNode == null) return null

    if ((mainNode.isObject && updateNode.isObject) && !(mainNode.isArray && updateNode.isArray)) {

      val first = mainNode.asInstanceOf[ObjectNode]
      val second = updateNode.asInstanceOf[ObjectNode]

      val firstFieldNames = first.fieldNames()
      val secondFieldNames = second.fieldNames()

      while (firstFieldNames.hasNext && secondFieldNames.hasNext) {
        val firstField = new FieldResolver(firstFieldNames.next())
        val secondField = new FieldResolver(secondFieldNames.next())

        if (firstField.name == secondField.name) {
          val node = first.get(firstField.name)
          val node2 = second.get(secondField.name)

          if (firstField.hasArrayChild && firstField.hasObjectChild) {
            val fields1 = node.fields()
            val fields2 = node2.fields()
          }

          if (firstField.hasObjectChild) {
            merge(node, node)
          }



        } else {
          first.asInstanceOf[ObjectNode].put(secondField.name, second.get(secondField.name))
        }

      }
    }

    else if(mainNode.isArray && updateNode.isArray) {
      (0 to mainNode.size()).foreach { i =>
        merge(mainNode.get(i), updateNode.get(i))
      }
    }

    return mainNode

  }


  private def resolveJsonRec(path: util.List[String], endNode: Object): Object = {
    if (path.isEmpty()) return endNode

    val reverse = path.reverse

    var first = true
    var lastValue: Object = null
    reverse.forEach { p =>
      val field = new FieldResolver(p)
      val newParent = new collection.mutable.HashMap[String, Object]()
      if (field.hasArrayChild) {
        newParent += (field.name -> (if (first) endNode else List(lastValue).asJava))
      }
      if (field.hasObjectChild) {
        newParent += (field.name -> (if (first) endNode else lastValue))
      }
      if (field.hasValueChild) {
        newParent += (field.name -> (if (first) endNode else lastValue))
      }

      first = false
      lastValue = newParent.asJava
    }

    return lastValue
  }

  def resolveJson(path: util.List[String], values: util.List[String]): String = {
    val reverse = path.reverse
    val str = reverse.get(0)

    val field = new FieldResolver(str)

    var endNode: Object = null

    if (field.hasArrayChild) {
      val map = new collection.mutable.HashMap[String, util.List[String]]()
      map.put(field.name, values)
      endNode = map.asJava
    }

    if (field.hasObjectChild) {
      val java = values.map { s =>
        val map = new collection.mutable.HashMap[String, Object]()
        map += (field.name -> s)
        map.asJava
      }
      endNode = (if (java.size > 1) java.asJava else java.get(0))
    }

    if (field.hasValueChild) {

      if (values.size() == 1) {
        val map = new collection.mutable.HashMap[String, Object]()
        val value = values.get(0)
        map += (field.name -> value)
        endNode = map.asJava
      } else {
        val list = collection.mutable.ArrayBuffer[util.Map[String, String]]()
        values.forEach { v =>
          val map = new collection.mutable.HashMap[String, String]()
          map += (field.name -> v)
          list += map.asJava
        }
        endNode = list.asJava
      }

    }



    val mapper = new ObjectMapper()
    val stringToObject = resolveJsonRec(path.subList(0, path.size() - 1), endNode)

    mapper.writeValueAsString(stringToObject)
  }

}
