package org.sens.validator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{AggregationConcept, Attribute, Concept, DataSourceConcept, Order, ParentConcept, ExpressionIdent}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, AnonymousFunctionConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression._
import org.sens.core.expression.literal.{BasicTypeLiteral, _}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.{ConceptDefinition, FunctionDefinition, NOP, Return, VariableDefinition}
import org.sens.parser.{ElementNotFoundException, ValidationContext, WrongFunctionArgumentsException}

import scala.collection.immutable.Nil
import scala.util.{Failure, Success}

class ExpressionValidationTests extends AnyFlatSpec with Matchers {

  "Literals" should "be validated correctly" in {
    val context = ValidationContext()
    val bl = BooleanLiteral(true)
    bl.validateAndRemoveVariablePlaceholders(context) should equal (Success(bl))
    val il = IntLiteral(1)
    il.validateAndRemoveVariablePlaceholders(context) should equal(Success(il))
    val fl = FloatLiteral(0.5)
    fl.validateAndRemoveVariablePlaceholders(context) should equal(Success(fl))
    val sl = StringLiteral("a")
    sl.validateAndRemoveVariablePlaceholders(context) should equal(Success(sl))
    val nl = NullLiteral()
    nl.validateAndRemoveVariablePlaceholders(context) should equal(Success(nl))
    val ll = ListLiteral(IntLiteral(1) :: StringLiteral("plus") :: Nil)
    ll.validateAndRemoveVariablePlaceholders(context) should equal(Success(ll))
    val ml = MapLiteral(Map(StringLiteral("key1") -> StringLiteral("strValue"), StringLiteral("key2") -> BooleanLiteral(true)))
    ml.validateAndRemoveVariablePlaceholders(context) should equal(Success(ml))
    val tl = BasicTypeLiteral(SensBasicTypes.INT_TYPE)
    tl.validateAndRemoveVariablePlaceholders(context) should equal(Success(tl))
  }

  "Named placeholders" should "be validated correctly" in {
    val context1 = ValidationContext()
    NamedElementPlaceholder("a").validateAndRemoveVariablePlaceholders(context1) should equal(
      Failure(ElementNotFoundException("a"))
    )
    val context2 = ValidationContext()
    context2.addVariable(VariableDefinition("a", None))
    NamedElementPlaceholder("a").validateAndRemoveVariablePlaceholders(context2) should equal (
      Success(Variable("a"))
    )
    val context3 = ValidationContext()
    context3.addFunction(FunctionDefinition("a", Nil, NOP()))
    NamedElementPlaceholder("a").validateAndRemoveVariablePlaceholders(context3) should equal (
      Success(FunctionReference("a"))
    )
    val context4 = ValidationContext()
    val conceptDef = ConceptDefinition(AggregationConcept(
      "myRel",
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) ::
      ParentConcept(ConceptReference("concept2"), Some("c2"), Map("id" -> ConceptAttribute("c1" :: Nil, "c1Ref")), Nil) :: Nil,
      None,
      Nil
    ))
    context4.addConcept(conceptDef)
    NamedElementPlaceholder("myRel").validateAndRemoveVariablePlaceholders(context4) should equal (
      Success(ConceptReference("myRel"))
    )
    val context5 = ValidationContext()
    context5.setCurrentConcept(DataSourceConcept(
      "someConcept",
      Attribute("a", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    ))
    NamedElementPlaceholder("a").validateAndRemoveVariablePlaceholders(context5) should equal (
      Success(ConceptAttribute(Nil, "a"))
    )
    val context6 = ValidationContext()
    context6.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("myRel"), Some("a"), Map(), Nil) :: Nil
    ).build())
    NamedElementPlaceholder("a").validateAndRemoveVariablePlaceholders(context6) should equal (
      Success(ConceptObject("a"))
    )

    val context7 = ValidationContext()
    context7.addConcept(conceptDef)
    context7.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("myRel"), Some("a"), Map(), Nil) :: Nil
    ).build())
    NamedElementPlaceholder("c1").validateAndRemoveVariablePlaceholders(context7) should equal (
      Success(ConceptAttribute("a" :: Nil, "c1"))
    )
    context7.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("myRel"), Some("a"), Map(), Nil) ::
        ParentConcept(ConceptReference("myRel"), Some("b"), Map(), Nil) :: Nil
    ).build())
    val res = NamedElementPlaceholder("c1").validateAndRemoveVariablePlaceholders(context7)
    res.isFailure should be (true)
    res.failed.get.getMessage should equal ("Attribute c1 defined in more than one parent concept")

    NamedElementPlaceholder("length").validateAndRemoveVariablePlaceholders(context7) should equal (
      Success(FunctionReference("length"))
    )

    val context8 = ValidationContext()
    context8.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("parentName")), Map()), Some("a"), Map(), Nil) :: Nil
    ).genericParameters("parentName" :: Nil)
      .build())
    NamedElementPlaceholder("parentName").validateAndRemoveVariablePlaceholders(context8) should equal(
      Success(GenericParameter("parentName"))
    )
  }

  "Variables" should "be validated correctly" in {
    val context = ValidationContext()
    Variable("a").validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariable(VariableDefinition("a", None))
    Variable("a").validateAndRemoveVariablePlaceholders(context) should equal(Success(Variable("a")))
  }

  "Concept attributes" should "be validated correctly" in {
    val context = ValidationContext()
    val attr1 = ConceptAttribute(Nil, "a")
    attr1.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.setCurrentConcept(DataSourceConcept(
      "someConcept",
      Attribute("a", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    ))
    attr1.validateAndRemoveVariablePlaceholders(context) should equal(Success(attr1))

    val attr2 = ConceptAttribute("c" :: Nil, "a")
    attr2.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("c"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "concept1",
      Attribute("a", None, Nil) ::
        Attribute("b", None, Nil) ::
        Attribute("c", None, Nil) ::
        Attribute("d", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c"), Map(), Nil) :: Nil
    ).build())
    attr2.validateAndRemoveVariablePlaceholders(context) should equal(Success(attr2))
  }

  "Concept attributes chain" should "be validated correctly" in {
    val context = ValidationContext()

    val attr = ConceptAttribute("r" :: "f" :: Nil, "val")
    attr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("r"))
    )

    context.setCurrentConcept(Concept.builder(
      "metrics",
      Attribute("total", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("r" :: "f" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("factDimRel"), Some("r"), Map(), Nil) :: Nil)
      .build()
    )

    attr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("factDimRel"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someFact",
      Attribute("dim", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("time", None, Nil) :: Nil,
      FileDataSource("someFactFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someDim",
      Attribute("id", None, Nil) ::
        Attribute("val", None, Nil) :: Nil,
      FileDataSource("someDimFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(AggregationConcept.builder(
      "factDimRel",
      ParentConcept(ConceptReference("someFact"), Some("f"), Map(), Nil) ::
        ParentConcept(ConceptReference("someDim"), Some("d"), Map("id" -> ConceptAttribute("f" :: Nil, "dim")), Nil) :: Nil
    ).build()))

    attr.validateAndRemoveVariablePlaceholders(context).get should equal(
      attr
    )
  }

  "Concept objects" should "be validated correctly" in {
    val context = ValidationContext()
    val obj = ConceptObject("a")
    obj.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptA"), Some("a"), Map(), Nil) :: Nil
    ).build())
    obj.validateAndRemoveVariablePlaceholders(context) should equal(Success(obj))
  }

  "Function references" should "be validated correctly" in {
    val context = ValidationContext()
    val func = FunctionReference("a")
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addFunction(FunctionDefinition("a", Nil, NOP()))
    func.validateAndRemoveVariablePlaceholders(context) should equal(Success(func))
  }

  "Concept references" should "be validated correctly" in {
    val context = ValidationContext()
    val conRef = ConceptReference("myRel")
    conRef.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("myRel"))
    )
    val conceptDef = ConceptDefinition(AggregationConcept(
      "myRel",
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("concept2"), Some("c2"), Map("id" -> ConceptAttribute("c1" :: Nil, "c1Ref")), Nil) :: Nil,
      None,
      Nil
    ))
    context.addConcept(conceptDef)
    conRef.validateAndRemoveVariablePlaceholders(context) should equal(Success(conRef))
  }

  "Generic parameters" should "be validated correctly" in {
    val context = ValidationContext()
    val gp = GenericParameter("a")
    gp.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.setCurrentConcept(Concept.builder(
      "myConcept",
      Attribute("val", None, Nil) :: Nil,
      ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("a")), Map()), None, Map(), Nil) :: Nil
    ).genericParameters("a" :: Nil).build())
    gp.validateAndRemoveVariablePlaceholders(context) should equal(Success(gp))
  }


  "Collection item" should "be validated correctly" in {
    val context = ValidationContext()
    val colItem = CollectionItem(NamedElementPlaceholder("a"), NamedElementPlaceholder("b") :: Nil)
    colItem.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil)
    colItem.validateAndRemoveVariablePlaceholders(context).get should equal (
      CollectionItem(Variable("a"), Variable("b") :: Nil)
    )
  }

  "Function call" should "be validated correctly" in {
    val context = ValidationContext()
    val funcCall = FunctionCall(FunctionReference("a"), NamedElementPlaceholder("b") :: Nil)
    funcCall.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addFunction(FunctionDefinition("a", "b" :: Nil, NOP()))
    context.addVariable(VariableDefinition("b", None))
    funcCall.validateAndRemoveVariablePlaceholders(context).get should equal (
      FunctionCall(FunctionReference("a"), Variable("b") :: Nil)
    )

    FunctionCall(FunctionReference("a"), Nil).validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(WrongFunctionArgumentsException("a", 0))
    )
  }

  "If expression" should "be validated correctly" in {
    val context = ValidationContext()
    val ifExpr = If(NamedElementPlaceholder("a"), NamedElementPlaceholder("b"), NamedElementPlaceholder("c"))
    ifExpr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: VariableDefinition("c", None) :: Nil)
    ifExpr.validateAndRemoveVariablePlaceholders(context).get should equal (
      If(Variable("a"), Variable("b"), Variable("c"))
    )
  }

  "List initialization" should "be validated correctly" in {
    val context = ValidationContext()
    val listExpr = ListInitialization(NamedElementPlaceholder("a") :: NamedElementPlaceholder("b") :: Nil)
    listExpr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil)
    listExpr.validateAndRemoveVariablePlaceholders(context).get should equal (
      ListInitialization(Variable("a") :: Variable("b") :: Nil)
    )
  }

  "Map initialization" should "be validated correctly" in {
    val context = ValidationContext()
    val mapExpr = MapInitialization(Map(
      NamedElementPlaceholder("a") -> NamedElementPlaceholder("b"),
      NamedElementPlaceholder("c") -> NamedElementPlaceholder("d")
    ))
    mapExpr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(
      VariableDefinition("a", None) ::
      VariableDefinition("b", None) ::
      VariableDefinition("c", None)  ::
      VariableDefinition("d", None) :: Nil
    )
    mapExpr.validateAndRemoveVariablePlaceholders(context).get should equal (
      MapInitialization(Map(
        Variable("a") -> Variable("b"),
        Variable("c") -> Variable("d")
      ))
    )
  }

  "Method invocation" should "be validated correctly" in {
    val context = ValidationContext()
    val methodCall = MethodInvocation("a" :: "nestedFunc" :: Nil, NamedElementPlaceholder("c") :: Nil)
    methodCall.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addFunction(FunctionDefinition("a", "b" :: Nil, NOP()))
    context.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("sourceTable"), Some("a"), Map(), Nil) :: Nil
    ).build())
    methodCall.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("sourceTable"))
    )

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "sourceTable",
      Attribute("nestedFunc", None, Nil) ::
        Attribute("someAttr", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    methodCall.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("c"))
    )

    context.addVariable(VariableDefinition("c", None))
    methodCall.validateAndRemoveVariablePlaceholders(context) should equal (
      Success(MethodInvocation("a" :: "nestedFunc" :: Nil, Variable("c") :: Nil))
    )
  }

  "Switch expression" should "be validated correctly" in {
    val context = ValidationContext()
    val switchExpr = Switch(
      Map(
        NamedElementPlaceholder("a") -> NamedElementPlaceholder("b"),
        NamedElementPlaceholder("c") -> NamedElementPlaceholder("d")
      ),
      NamedElementPlaceholder("e")
    )
    switchExpr.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(
      VariableDefinition("a", None) ::
      VariableDefinition("b", None) ::
      VariableDefinition("c", None)  ::
      VariableDefinition("d", None) ::
      VariableDefinition("e", None) :: Nil
    )
    switchExpr.validateAndRemoveVariablePlaceholders(context).get should equal (
      Switch(
        Map(
          Variable("a") -> Variable("b"),
          Variable("c") -> Variable("d")
        ),
        Variable("e")
      )
    )
  }

  "Anonymous function definitions" should "be validated correctly" in {
    val context = ValidationContext()
    val func = AnonymousFunctionDefinition(
      "a" :: Nil,
      Return(Some(
        FunctionCall(
          FunctionReference("f"),
          NamedElementPlaceholder("a") :: IntLiteral(1) :: Nil
        )
      ))
    )
    func.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("f")))

    context.addFunction(FunctionDefinition("f", "a" :: "b" :: Nil, NOP()))
    func.validateAndRemoveVariablePlaceholders(context).get should equal (
      AnonymousFunctionDefinition(
        "a" :: Nil,
        Return(Some(
          FunctionCall(
            FunctionReference("f"),
            Variable("a") :: IntLiteral(1) :: Nil
          )
        ))
      )
    )
  }

  "Anonymous concept definitions" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = AnonymousConceptDefinition(
      Attribute("attr1", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute("c1" :: Nil, "val"), NamedElementPlaceholder("attr1"))
      )),
      NamedElementPlaceholder("attr1") :: Nil,
      Some(GreaterThan(NamedElementPlaceholder("attr1"), IntLiteral(10))),
      Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Nil
    )
    conDef.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("concept1")))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "concept1",
      Attribute("id", None, Nil) ::
      Attribute("val", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "concept2",
      Attribute("id", None, Nil) ::
      Attribute("val", None, Nil) ::
      Attribute("concept1id", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      AnonymousConceptDefinition(
        Attribute("attr1", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
        Some(And(
          Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
          Equals(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute(Nil, "attr1"))
        )),
        ConceptAttribute(Nil, "attr1") :: Nil,
        Some(GreaterThan(ConceptAttribute(Nil, "attr1"), IntLiteral(10))),
        Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil,
        Some(10),
        Some(100),
        Nil
      )
    )

    val conDef1 = AnonymousConceptDefinition.builder(
      Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(NamedElementPlaceholder("id"), IntLiteral(0)))
      .build

    conDef1.validateAndRemoveVariablePlaceholders(context).get should equal (
      AnonymousConceptDefinition.builder(
        Nil,
        ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), IntLiteral(0)))
        .build
    )

    val conDef2 = AnonymousConceptDefinition(
      Attribute("attr1", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute("c1" :: Nil, "val"), NamedElementPlaceholder("attr1"))
      )),
      NamedElementPlaceholder("unknown") :: Nil,
      None,
      Order(NamedElementPlaceholder("attr1"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Nil
    )
    conDef2.validateAndRemoveVariablePlaceholders(context) should equal (
      Failure(ElementNotFoundException("unknown"))
    )
  }

  "Anonymous Function Concept definition" should "be validated correctly" in {
    val context = ValidationContext()
    val conDef = AnonymousFunctionConceptDefinition(
      Return(Some(
        FunctionCall(
          FunctionReference("f"),
          ConceptAttribute("c" :: Nil, "attr1") :: NamedElementPlaceholder("n") :: Nil
        )
      ))
    )
    conDef.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("f")))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "concept1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    context.setCurrentConcept(Concept.builder(
      "someConcept",
      Attribute("someAttr", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c"), Map(), Nil) :: Nil
    ).build())
    context.addFunction(FunctionDefinition("f", "a" :: "b" :: Nil, NOP()))
    context.addVariable(VariableDefinition("n", None))
    conDef.validateAndRemoveVariablePlaceholders(context).get should equal (
      AnonymousFunctionConceptDefinition(
        Return(Some(
          FunctionCall(
            FunctionReference("f"),
            ConceptAttribute("c" :: Nil, "attr1") :: Variable("n") :: Nil
          )
        ))
      )
    )
  }

  "Standard Sens functions calls" should "be validated correctly" in {
    val context = ValidationContext()
    val funcCall = FunctionCall(FunctionReference("length"), NamedElementPlaceholder("a") :: Nil)
    funcCall.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariable(VariableDefinition("a", None))
    funcCall.validateAndRemoveVariablePlaceholders(context).get should equal (
      FunctionCall(FunctionReference("length"), Variable("a") :: Nil)
    )
  }

  "Window functions calls" should "be validated correctly" in {
    val context = ValidationContext()
    WindowFunction(
      WindowFunctions.SUM,
      Nil,
      Nil,
      Nil,
      (None, None),
      (None, None)
    ).validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(WrongFunctionArgumentsException("sum", 0))
    )

    val func = WindowFunction(
      WindowFunctions.SUM,
      NamedElementPlaceholder("a") :: Nil,
      NamedElementPlaceholder("b") :: Nil,
      Order(NamedElementPlaceholder("c"), Order.ASC) :: Nil,
      (Some(NamedElementPlaceholder("d")), Some(NamedElementPlaceholder("e"))),
      (None, None)
    )
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariable(VariableDefinition("a", None))
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("b"))
    )

    context.addVariable(VariableDefinition("b", None))
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("c"))
    )

    context.addVariable(VariableDefinition("c", None))
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("d"))
    )

    context.addVariable(VariableDefinition("d", None))
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("e"))
    )

    context.addVariable(VariableDefinition("e", None))
    func.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(
        WindowFunction(
          WindowFunctions.SUM,
          Variable("a") :: Nil,
          Variable("b") :: Nil,
          Order(Variable("c"), Order.ASC) :: Nil,
          (Some(Variable("d")), Some(Variable("e"))),
          (None, None)
        )
      )
    )

    val func1 = WindowFunction(
      WindowFunctions.SUM,
      NamedElementPlaceholder("a") :: Nil,
      NamedElementPlaceholder("b") :: Nil,
      Order(NamedElementPlaceholder("c"), Order.ASC) :: Nil,
      (None, None),
      (Some(NamedElementPlaceholder("f")), Some(NamedElementPlaceholder("g")))
    )

    func1.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("f"))
    )

    context.addVariable(VariableDefinition("f", None))
    func1.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("g"))
    )

    context.addVariable(VariableDefinition("g", None))
    func1.validateAndRemoveVariablePlaceholders(context) should equal(
      Success(
        WindowFunction(
          WindowFunctions.SUM,
          Variable("a") :: Nil,
          Variable("b") :: Nil,
          Order(Variable("c"), Order.ASC) :: Nil,
          (None, None),
          (Some(Variable("f")), Some(Variable("g")))
        )
      )
    )

    val fail1 = WindowFunction(
      WindowFunctions.SUM,
      NamedElementPlaceholder("a") :: Nil,
      NamedElementPlaceholder("b") :: Nil,
      Order(NamedElementPlaceholder("c"), Order.ASC) :: Nil,
      (Some(NamedElementPlaceholder("d")), Some(NamedElementPlaceholder("e"))),
      (Some(NamedElementPlaceholder("f")), Some(NamedElementPlaceholder("g")))
    ).validateAndRemoveVariablePlaceholders(context)
    fail1.isFailure should equal (true)
    fail1.failed.get.getMessage should equal ("Window function cannot have ROWS and RANGE at the same time")

    val fail2 = WindowFunction(
      WindowFunctions.SUM,
      NamedElementPlaceholder("a") :: Nil,
      NamedElementPlaceholder("b") :: Nil,
      Nil,
      (None, None),
      (Some(NamedElementPlaceholder("f")), Some(NamedElementPlaceholder("g")))
    ).validateAndRemoveVariablePlaceholders(context)
    fail2.isFailure should equal(true)
    fail2.failed.get.getMessage should equal("Window function with RANGE requires exactly one ORDER BY column")
  }


}
