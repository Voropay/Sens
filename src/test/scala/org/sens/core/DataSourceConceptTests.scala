package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, DataSourceConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.literal.StringLiteral
import org.sens.parser.ValidationContext

class DataSourceConceptTests extends AnyFlatSpec with Matchers {

  "FileDataSource" should "be formatted in Sens correctly" in {
    val dsDef = FileDataSource("someFile", FileFormats.CSV)
    dsDef.toSensString should equal ("CSV file \"someFile\"")
  }

  "DataSource Concepts" should "correctly return name and attributes" in {
    val conDef = DataSourceConcept(
      "myconcept",
      Attribute("attr1", None, Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Annotation.OPTIONAL :: Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )

    conDef.name should equal ("myconcept")
    conDef.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2", "attr3", "attr4"))
    conDef.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", None, Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Annotation.OPTIONAL :: Nil) :: Nil)
  }

  "DataSource Concepts" should "be formatted in Sens correctly" in {
    val conDef = DataSourceConcept(
      "myconcept",
      Attribute("attr1", None, Annotation("NotNull", Map()) :: Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Annotation("Owner", Map("Email" -> StringLiteral("team@company.org"))) :: Nil
    )
    val conDefStr = "@Owner (Email = \"team@company.org\")\n" +
    "datasource myconcept (@NotNull attr1,\n" +
    "attr2,\n" +
    "attr3,\n" +
    "attr4)\n" +
    "from CSV file \"someFile\""
    conDef.toSensString should equal (conDefStr)
  }
}
