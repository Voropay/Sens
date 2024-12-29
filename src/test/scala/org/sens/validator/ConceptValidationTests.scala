package org.sens.validator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, GenericParameter, NamedElementPlaceholder, Variable}
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.statement.{ConceptDefinition, FunctionDefinition, NOP, Return, VariableDefinition}
import org.sens.parser.{AttributeNamesDoNotMatch, ElementNotFoundException, GenericDefinitionException, ValidationContext}

import scala.util.{Failure, Success}

class ConceptValidationTests extends AnyFlatSpec with Matchers  {

  "Annotations" should "be validated correctly" in {
    val context = ValidationContext()
    val an = Annotation("UniqueKey", Map(
      "name" -> NamedElementPlaceholder("attr")
    ))

    an.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("attr"))
    )

    context.setCurrentConcept(DataSourceConcept(
      "someConcept",
      Attribute("attr", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    ))

    an.validateAndRemoveVariablePlaceholders(context).get should equal(
      Annotation("UniqueKey", Map(
        "name" -> ConceptAttribute(Nil, "attr")
      ))
    )
  }

  "Attributes" should "be validated correctly" in {
    val context = ValidationContext()
    val attr = Attribute(
      "s",
      Some(Multiply(NamedElementPlaceholder("pi"),
        Multiply(NamedElementPlaceholder("r"), NamedElementPlaceholder("r")))
      ),
      new Annotation("Default", Map("value" -> FunctionCall(FunctionReference("my_current_date"), Nil))) :: Nil
    )
    attr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("pi"))
    )

    context.addVariable(VariableDefinition("pi", None))
    context.setCurrentConcept(DataSourceConcept(
      "someConcept",
      Attribute("r", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    ))
    attr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("my_current_date"))
    )

    context.addFunction(FunctionDefinition("my_current_date", Nil, NOP()))
    attr.validateAndRemoveVariablePlaceholders(context).get should equal (
      Attribute(
        "s",
        Some(Multiply(Variable("pi"),
          Multiply(ConceptAttribute(Nil, "r"), ConceptAttribute(Nil, "r")))
        ),
        new Annotation("Default", Map("value" -> FunctionCall(FunctionReference("my_current_date"), Nil))) :: Nil)
    )
  }

  "Parent Concepts" should "be validated correctly" in {
    val context = ValidationContext()
    val pc = ParentConcept(
      ConceptReference("conceptA"),
      None,
      Map(
        "id" -> NamedElementPlaceholder("id")
      ),
      new Annotation("UseIndex", Map("name" -> NamedElementPlaceholder("indexName"))) :: Nil
    )
    pc.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptA"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("someAttr", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(
      InheritedConcept(
        "conceptA",
        Nil,
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil,
        Attribute("a", None, Nil) :: Nil,
        Nil,
        Some(Equals(NamedElementPlaceholder("a"), IntLiteral(0))),
        Nil,
        None,
        None,
        Nil
      )
    ))
    pc.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("id"))
    )

    context.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("id", None, Nil) :: Nil,
      pc :: Nil
    ).build())
    pc.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("indexName"))
    )

    context.addVariable(VariableDefinition("indexName", None))
    pc.validateAndRemoveVariablePlaceholders(context).get should equal (
      ParentConcept(
        ConceptReference("conceptA"),
        None,
        Map(
          "id" -> ConceptAttribute(Nil, "id")
        ),
        new Annotation("UseIndex", Map("name" -> Variable("indexName"))) :: Nil
      )
    )
  }

  "Generic Concept Reference" should "be validated correctly" in {
    val context = ValidationContext()
    val gcr = GenericConceptReference(ConstantIdent("genericConcept"), Map("param" -> NamedElementPlaceholder("myParam")))
    gcr.validateAndRemoveVariablePlaceholders(context) should equal (
      Failure(ElementNotFoundException("myParam"))
    )

    context.setCurrentConcept(Concept.builder(
      "myConcept",
      Attribute("val", None, Nil) :: Nil,
      ParentConcept(gcr, None, Map(), Nil) :: Nil
    ).genericParameters("myParam" :: Nil).build())
    gcr.validateAndRemoveVariablePlaceholders(context) should equal (
      Failure(ElementNotFoundException("genericConcept"))
    )

    context.addConcept(ConceptDefinition(
      Concept.builder(
        "genericConcept",
        Attribute("val", None, Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(GenericParameter("anotherParam")), Map()),
          None, Map(), Nil) :: Nil
      ).genericParameters("anotherParam" :: Nil).build()
    ))
    gcr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(GenericDefinitionException("Generic parameters do not match for concept genericConcept"))
    )

    context.addConcept(ConceptDefinition(
      Concept.builder(
        "genericConcept",
        Attribute("val", None, Nil) :: Nil,
        ParentConcept(
          ConceptReference("someParentConcept"),
          None, Map(), Nil) :: Nil
      ).build()
    ))
    gcr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(GenericDefinitionException("Generic parameters do not match for concept genericConcept"))
    )

    context.addConcept(ConceptDefinition(
      Concept.builder(
        "genericConcept",
        Attribute("val", None, Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(GenericParameter("param")), Map()),
          None, Map(), Nil) :: Nil
      ).genericParameters("param" :: Nil).build()
    ))
    gcr.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(GenericConceptReference(ConstantIdent("genericConcept"), Map("param" -> GenericParameter("myParam"))))
    )

    val gcr1 = GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("myGenericName")), Map())
    gcr1.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("myGenericName")), Map()))
    )

    context.setCurrentConcept(Concept.builder(
      "myConcept",
      Attribute("val", None, Nil) :: Nil,
      ParentConcept(gcr, None, Map(), Nil) :: Nil
    ).genericParameters("myGenericName" :: Nil).build())
    gcr1.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(GenericConceptReference(ExpressionIdent(GenericParameter("myGenericName")), Map()))
    )
  }

  "Order" should "be validated correctly" in {
    val context = ValidationContext()
    val order = Order(NamedElementPlaceholder("a"), Order.ASC)
    order.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.setCurrentConcept(DataSourceConcept(
      "someConcept",
      Attribute("a", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    ))
    order.validateAndRemoveVariablePlaceholders(context).get should equal (
      Order(ConceptAttribute(Nil, "a"), Order.ASC)
    )
  }

  "Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = Concept.builder(
      "conceptA",
      Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("pc"), Map("attr2" -> NamedElementPlaceholder("attr2")), Nil) :: Nil)
      .attributeDependencies(Equals(NamedElementPlaceholder("attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
      .groupByAttributes(NamedElementPlaceholder("attr1") :: Nil)
      .groupDependencies(GreaterThan(NamedElementPlaceholder("attr1"), IntLiteral(10)))
      .orderByAttributes(Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil)
      .annotations(Annotation("UniqueKey", Map("name" -> NamedElementPlaceholder("attr1"))) :: Nil)
      .build

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
          Attribute("attr2", None, Nil) ::
          Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "attr1"), IntLiteral(10)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .annotations(Annotation("UniqueKey", Map("name" -> ConceptAttribute(Nil, "attr1"))) :: Nil)
        .build()
    )

    val conDef1 = Concept.builder(
      "conceptA",
      Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("pc"), Map("attr2" -> NamedElementPlaceholder("attr2")), Nil) :: Nil)
      .attributeDependencies(Equals(NamedElementPlaceholder("attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
      .groupByAttributes(NamedElementPlaceholder("attrNotFound") :: Nil)
      .orderByAttributes(Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil)
      .build

    conDef1.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("attrNotFound"))
    )
  }

  "Generic Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val genConDef = Concept.builder(
      "genericConcept",
      Attribute("key", Some(NamedElementPlaceholder("attr1")), Nil) ::
        Attribute("val", Some(NamedElementPlaceholder("attr2")), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("source")), Map()),
        None, Map(), Nil) :: Nil
    ).genericParameters("source" :: Nil).build()
    val genConDefValidated = genConDef.validateAndRemoveVariablePlaceholders(context)
    genConDefValidated should equal (
      Success(Concept.builder(
        "genericConcept",
        Attribute("key", Some(NamedElementPlaceholder("attr1")), Nil) ::
          Attribute("val", Some(NamedElementPlaceholder("attr2")), Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(GenericParameter("source")), Map()),
          None, Map(), Nil) :: Nil
      ).genericParameters("source" :: Nil).build()
    ))
    context.addConcept(ConceptDefinition(genConDefValidated.get))

    val conDef = Concept.builder(
      "myConcept",
      Attribute("key", None, Nil) ::
        Attribute("total", Some(FunctionCall(FunctionReference("sum"), NamedElementPlaceholder("val") :: Nil)), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ConstantIdent("genericConcept"), Map("source" -> StringLiteral("sourceTable"))),
        None, Map(), Nil) :: Nil
    ).groupByAttributes(NamedElementPlaceholder("key") :: Nil).build()
    conDef.validateAndRemoveVariablePlaceholders(context) should equal (
      Failure(ElementNotFoundException("sourceTable"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "sourceTable",
      Attribute("key", None, Nil) ::
        Attribute("val", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("attr1"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "sourceTable",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(Concept.builder(
        "myConcept",
        Attribute("key", None, Nil) ::
          Attribute("total", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("genericConcept" :: Nil, "val") :: Nil)), Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ConstantIdent("genericConcept"), Map("source" -> StringLiteral("sourceTable"))),
          None, Map(), Nil) :: Nil
      ).groupByAttributes(ConceptAttribute(Nil, "key") :: Nil).build()
    ))
  }

  "Anonymous generic Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val genConDef = Concept.builder(
      "genericConcept",
      Attribute("key", Some(ConceptAttribute("a" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("a" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(
        AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
            Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("source")), Map()),
          None, Map(), Nil) :: Nil
        ).build(),
        Some("a"), Map(), Nil) :: Nil
    ).genericParameters("source" :: Nil).build()

    val genConDefValidated = genConDef.validateAndRemoveVariablePlaceholders(context)
    genConDefValidated should equal(
      Success(Concept.builder(
        "genericConcept",
        Attribute("key", Some(ConceptAttribute("a" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("a" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("attr1", None, Nil) ::
              Attribute("attr2", None, Nil) :: Nil,
            ParentConcept(
              GenericConceptReference(ExpressionIdent(GenericParameter("source")), Map()),
              None, Map(), Nil) :: Nil
          ).build(),
          Some("a"), Map(), Nil) :: Nil
      ).genericParameters("source" :: Nil).build()
      ))
  }

  "Generic Inherited Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val genConDef = InheritedConcept.builder(
      "genericConcept",
      ParentConcept(
        GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("source")), Map()),
        None, Map(), Nil) :: Nil
    ).overriddenAttributes(Attribute("key", Some(NamedElementPlaceholder("attr1")), Nil) ::
      Attribute("val", Some(NamedElementPlaceholder("attr2")), Nil) :: Nil)
      .genericParameters("source" :: Nil).build()
    val genConDefValidated = genConDef.validateAndRemoveVariablePlaceholders(context)
    genConDefValidated should equal(
      Success(InheritedConcept.builder(
        "genericConcept",
        ParentConcept(
          GenericConceptReference(ExpressionIdent(GenericParameter("source")), Map()),
          None, Map(), Nil) :: Nil
      ).overriddenAttributes(Attribute("key", Some(NamedElementPlaceholder("attr1")), Nil) ::
        Attribute("val", Some(NamedElementPlaceholder("attr2")), Nil) :: Nil)
        .genericParameters("source" :: Nil).build()
      ))
    context.addConcept(ConceptDefinition(genConDefValidated.get))

    val conDef = InheritedConcept.builder(
      "myConcept",
      ParentConcept(
        GenericConceptReference(ConstantIdent("genericConcept"), Map("source" -> StringLiteral("sourceTable"))),
        None, Map(), Nil) :: Nil
    ).overriddenAttributes(Attribute("metric", Some(Multiply(NamedElementPlaceholder("key"), NamedElementPlaceholder("val"))), Nil) :: Nil)
      .build()
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("sourceTable"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "sourceTable",
      Attribute("key", None, Nil) ::
        Attribute("val", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("attr1"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "sourceTable",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(InheritedConcept.builder(
        "myConcept",
        ParentConcept(
          GenericConceptReference(ConstantIdent("genericConcept"), Map("source" -> StringLiteral("sourceTable"))),
          None, Map(), Nil) :: Nil
      ).overriddenAttributes(Attribute("metric", Some(Multiply(ConceptAttribute("genericConcept" :: Nil, "key"), ConceptAttribute("genericConcept" :: Nil, "val"))), Nil) :: Nil)
        .build()
      ))
  }

  "Parent Concept Attributes references" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = Concept.builder(
      "conceptA",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
      .attributeDependencies(Equals(NamedElementPlaceholder("attr3"), IntLiteral(0)))
      .groupByAttributes(NamedElementPlaceholder("attr2") :: Nil)
      .orderByAttributes(Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil)
      .build

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("conceptB" :: Nil, "attr3"), IntLiteral(0)))
        .groupByAttributes(ConceptAttribute(Nil, "attr2") :: Nil)
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    )
  }

  "Inherited Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map("attr2" -> ConceptAttribute("pcC" :: Nil, "attr2")), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map("attr1" -> NamedElementPlaceholder("attr1")), Nil) :: Nil)
      .overriddenAttributes(Attribute("attr1", None, Nil) ::
        Attribute("attr2", Some(Add(NamedElementPlaceholder("attr1"), IntLiteral(1))), Nil) ::
        Attribute("attr3", None, Nil) :: Nil)
      .removedAttributes(ConceptAttribute("pcB" :: Nil, "attr4") :: Nil)
      .additionalDependencies(Equals(NamedElementPlaceholder("attr3"), ConceptAttribute("pcC" :: Nil, "attr3")))
      .orderByAttributes(Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil)
      .annotations(Annotation("UniqueKey", Map("name" -> NamedElementPlaceholder("attr1"))) :: Nil)
      .build

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      InheritedConcept.builder(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map("attr2" -> ConceptAttribute("pcC" :: Nil, "attr2")), Nil) ::
         ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map("attr1" -> ConceptAttribute(Nil, "attr1")), Nil) :: Nil)
        .overriddenAttributes(Attribute("attr1", None, Nil) ::
          Attribute("attr2", Some(Add(ConceptAttribute(Nil, "attr1"), IntLiteral(1))), Nil) ::
          Attribute("attr3", None, Nil) :: Nil)
        .removedAttributes(ConceptAttribute("pcB" :: Nil, "attr4") :: Nil)
        .additionalDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pcC" :: Nil, "attr3")))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .annotations(Annotation("UniqueKey", Map("name" -> ConceptAttribute(Nil, "attr1"))) :: Nil)
        .build
    )

    val conDef2 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("a", Some(NamedElementPlaceholder("attr1")), Nil) :: Nil)
      .build

    conDef2.validateAndRemoveVariablePlaceholders(context).get should equal (
      InheritedConcept.builder(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
        .overriddenAttributes(Attribute("a", Some(ConceptAttribute("conceptB" :: Nil, "attr1")), Nil) :: Nil)
        .build
    )

    val conDef3 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
      .additionalDependencies(Equals(NamedElementPlaceholder("attr1"), IntLiteral(0)))
      .build

    conDef3.validateAndRemoveVariablePlaceholders(context).get should equal (
      InheritedConcept.builder(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
        .additionalDependencies(Equals(ConceptAttribute("conceptB" :: Nil, "attr1"), IntLiteral(0)))
        .build
    )
  }

  "Cube Concept definition" should "be validated correctly" in {
    val context = ValidationContext()

    val conDef = CubeConcept(
      "someCube",
      Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), NamedElementPlaceholder("val") :: Nil)), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), NamedElementPlaceholder("val") :: Nil)), Nil) :: Nil,
      Attribute("dim", None, Nil) :: Nil,
      ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
      Some(GreaterThan(NamedElementPlaceholder("time"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(NamedElementPlaceholder("totalVal"), IntLiteral(100))),
      Order(NamedElementPlaceholder("totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("someConcept"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someConcept",
      Attribute("dim", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("time", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      CubeConcept(
        "someCube",
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
        Attribute("dim", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
        Some(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01"))),
        Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
        Some(10),
        Some(100),
        Annotation("Public", Map()) :: Nil
      )
    )

    val conDef1 = CubeConcept.builder(
      "orderMetrics",
      Attribute("ordersCount", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("priceSum", Some(FunctionCall(FunctionReference("sum"), NamedElementPlaceholder("val") :: Nil)), Nil) :: Nil,
      Attribute("dim", None, Nil) :: Attribute("time", None, Nil) :: Nil,
      ParentConcept(ConceptReference("someConcept"), None, Map(), Nil) :: Nil)
      .annotations(Annotation(Annotation.MATERIALIZED, Map(Annotation.TYPE -> StringLiteral(Annotation.TABLE_TYPE))) :: Nil)
      .build()

    conDef1.validateAndRemoveVariablePlaceholders(context).get should equal (
      CubeConcept.builder(
        "orderMetrics",
        Attribute("ordersCount", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
          Attribute("priceSum", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("someConcept" :: Nil, "val") :: Nil)), Nil) :: Nil,
        Attribute("dim", None, Nil) :: Attribute("time", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), None, Map(), Nil) :: Nil)
        .annotations(Annotation(Annotation.MATERIALIZED, Map(Annotation.TYPE -> StringLiteral(Annotation.TABLE_TYPE))) :: Nil)
        .build()
    )

  }

  "Cube Inherited Concept definition" should "be validated correctly" in {
    val context = ValidationContext()

    val conDef = CubeInheritedConcept(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
      Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "category") :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: "c" :: Nil, "date"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(NamedElementPlaceholder("totalVal"), IntLiteral(100))),
      Order(NamedElementPlaceholder("totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("someCube"))
    )

    context.addConcept(ConceptDefinition(
      CubeConcept.builder(
        "someCube",
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), NamedElementPlaceholder("val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), NamedElementPlaceholder("val") :: Nil)), Nil) :: Nil,
        Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil)
        .annotations(Annotation("Private", Map()) :: Nil).build()
    ))

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("someConcept"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someConcept",
      Attribute("category", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal(
      CubeInheritedConcept(
        "anotherCube",
        ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
        ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
        ConceptAttribute("c" :: Nil, "category") :: Nil,
        Some(GreaterThan(ConceptAttribute("c" :: "c" :: Nil, "date"), StringLiteral("2024-01-01"))),
        Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
        Some(10),
        Some(100),
        Annotation("Public", Map()) :: Nil
      )
    )
  }

  "Aggregation Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = AggregationConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
      ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map("ref" -> NamedElementPlaceholder("pcC")), Nil) :: Nil,
      Some(Equals(
        FunctionCall(FunctionReference("getSomething"), NamedElementPlaceholder("pcB") :: Nil),
        StringLiteral("SomeValue")
      )),
      new Annotation("LastUpdated", Map("value" -> NamedElementPlaceholder("lastUpdated"))) :: Nil
    )
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("id", None, Nil) ::
        Attribute("val", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("id", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("ref", None, Nil)  :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("getSomething"))
    )

    context.addFunction(FunctionDefinition("getSomething", "obj" :: Nil, NOP()))
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("lastUpdated"))
    )

    context.addVariable(VariableDefinition("lastUpdated", None))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      AggregationConcept(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map("ref" -> ConceptAttribute(Nil, "pcC")), Nil) :: Nil,
        Some(Equals(
          FunctionCall(FunctionReference("getSomething"), ConceptAttribute(Nil, "pcB") :: Nil),
          StringLiteral("SomeValue")
        )),
        new Annotation("LastUpdated", Map("value" -> Variable("lastUpdated"))) :: Nil
      )
    )
  }

  "Function Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = FunctionConcept(
      "conceptA",
      Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil,
      AnonymousFunctionDefinition(
        Nil,
        Return(Some(FunctionCall(FunctionReference("read_csv_file"), NamedElementPlaceholder("attr2") :: Nil)))
      ),
      new Annotation("Log", Map("level" -> NamedElementPlaceholder("logLevel"))) :: Nil
    )
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    context.addFunction(FunctionDefinition("read_csv_file", "filePath" :: Nil, NOP()))
    context.addVariable(VariableDefinition("filePath", None))
    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("logLevel"))
    )

    context.addVariable(VariableDefinition("logLevel", None))
    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      FunctionConcept(
        "conceptA",
        Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil,
        AnonymousFunctionDefinition(
          Nil,
          Return(Some(FunctionCall(FunctionReference("read_csv_file"), ConceptAttribute(Nil, "attr2") :: Nil)))
        ),
        new Annotation("Log", Map("level" -> Variable("logLevel"))) :: Nil
      )
    )
  }

  "Union Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = UnionConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
        .build,
        None, Map(), Nil) :: Nil,
      new Annotation(
        "Materialized",
        Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> NamedElementPlaceholder("attr1"))
      ) :: Nil
    )

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptB",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptD"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .build
      ))

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptC",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", Some(ConceptAttribute("pc" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptE"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
      .build
      ))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal(
      UnionConcept(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("attr1", None, Nil) ::
            Attribute("attr2", None, Nil) ::
            Attribute("attr3", None, Nil) :: Nil,
            ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(ConceptAttribute("pcC" :: Nil, "attr4"), StringLiteral("someValue")))
            .build(),
            None, Map(), Nil) :: Nil,
        new Annotation(
          "Materialized",
          Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> ConceptAttribute(Nil, "attr1"))
        ) :: Nil
      )
    )

    val conDef2 = UnionConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
          Attribute("attr2", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )

    conDef2.validateAndRemoveVariablePlaceholders(context) should equal(Failure(
      AttributeNamesDoNotMatch()
    ))
  }

  "Intersect Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef1 = IntersectConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
        .build,
        None, Map(), Nil) :: Nil,
      new Annotation(
        "Materialized",
        Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> NamedElementPlaceholder("attr1"))
      ) :: Nil
    )

    conDef1.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptB",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptD"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .build
      ))

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptC",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", Some(ConceptAttribute("pc" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptE"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .build
      ))

    conDef1.validateAndRemoveVariablePlaceholders(context).get should equal(
      IntersectConcept(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
          Attribute("attr2", None, Nil) ::
          Attribute("attr3", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("pcC" :: Nil, "attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
        new Annotation(
          "Materialized",
          Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> ConceptAttribute(Nil, "attr1"))
        ) :: Nil
      )
    )

    val conDef2 = IntersectConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
          Attribute("attr2", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )

    conDef2.validateAndRemoveVariablePlaceholders(context) should equal(Failure(
      AttributeNamesDoNotMatch()
    ))
  }

  "Minus Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = MinusConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
        .build, None, Map(), Nil) :: Nil,
      new Annotation(
        "Materialized",
        Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> NamedElementPlaceholder("attr1"))
      ) :: Nil
    )

    conDef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptB",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("conceptD"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .build
      ))

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "conceptC",
        Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", Some(ConceptAttribute("pc" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptE"), Some("pc"), Map("attr2" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("pc" :: Nil, "attr3")))
        .build
      ))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal(
      MinusConcept(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
            Attribute("attr2", None, Nil) ::
            Attribute("attr3", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("pcC" :: Nil, "attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
        new Annotation(
          "Materialized",
          Map("type" -> StringLiteral("Incremental"), "Strategy" -> StringLiteral("Merge"), "UniqueKey" -> ConceptAttribute(Nil, "attr1"))
        ) :: Nil
      )
    )

    val conDef2 = MinusConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
          Attribute("attr2", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(NamedElementPlaceholder("attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )

    conDef2.validateAndRemoveVariablePlaceholders(context) should equal(Failure(
      AttributeNamesDoNotMatch()
    ))
  }
}
