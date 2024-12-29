package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, FunctionConcept, ParentConcept}
import org.sens.core.expression.FunctionCall
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.statement.Return
import org.sens.parser.ValidationContext

class FunctionConceptTests extends AnyFlatSpec with Matchers {

  "Function concepts" should "correctly return name and attributes" in {
    val conDef1 = FunctionConcept(
      "myconcept",
      Attribute("attr1", None, Nil) :: Attribute("attr2", None, Annotation.OPTIONAL :: Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil,
      AnonymousFunctionDefinition(
        Nil,
        Return(Some(FunctionCall(FunctionReference("read_csv_file"), StringLiteral("some_file_with_data.csv") :: Nil)))
      ),
      Nil
    )
    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2"))
    conDef1.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Annotation.OPTIONAL :: Nil) :: Nil
    )
  }

  "Function concepts" should "be formatted in Sens correctly" in {
    val conDef1 = FunctionConcept(
      "myconcept",
      Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil,
      AnonymousFunctionDefinition(
        Nil,
        Return(Some(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("some_file_with_data.csv") :: Nil)))
      ),
      Annotation("Group", Map("name" -> StringLiteral("admin"))) :: Nil
    )
    val conDef1Str =
      "@Group (name = \"admin\")\n" +
        "concept myconcept (attr1, attr2)\n" +
        "from concept1\n" +
        "by () => return read_csv_file(\"some_file_with_data.csv\")"
    conDef1.toSensString should equal(conDef1Str)

    val conDef2 = FunctionConcept(
      "myconcept",
      Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
      Nil,
      AnonymousFunctionDefinition(
        Nil,
        Return(Some(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("some_file_with_data.csv") :: Nil)))
      ),
      Nil
    )
    val conDef2Str =
      "concept myconcept (attr1, attr2)\n" +
        "by () => return read_csv_file(\"some_file_with_data.csv\")"
    conDef2.toSensString should equal(conDef2Str)

    val conDef3 = FunctionConcept(
      "myconcept",
      Nil,
      Nil,
      FunctionReference("read_myconcept_data"),
      Nil
    )
    val conDef3Str =
      "concept myconcept\n" +
        "by read_myconcept_data"
    conDef3.toSensString should equal(conDef3Str)
  }
}
