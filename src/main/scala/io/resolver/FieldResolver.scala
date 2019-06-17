package io.resolver

class FieldResolver(field: String) {
  val hasObjectChild = field.startsWith("_")
  val hasArrayChild = field.endsWith("π")
  val hasValueChild = !hasObjectChild & !hasArrayChild
  val name = field.replace("π", "").replace("_", "")


}
