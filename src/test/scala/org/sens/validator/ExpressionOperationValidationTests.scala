package org.sens.validator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Attribute, ParentConcept, DataSourceConcept}
import org.sens.core.datasource.{FileFormats, FileDataSource}
import org.sens.core.expression
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.{ConceptAttribute, ListInitialization, NamedElementPlaceholder, Variable}
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational.{All, Any, Exists, InList, InSubQuery, Unique}
import org.sens.core.statement.{FunctionDefinition, NOP, VariableDefinition}
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.Failure

class ExpressionOperationValidationTests extends AnyFlatSpec with Matchers {
  "Logical operations" should "be validated correctly" in {
    val context = ValidationContext()
    val andOp = And(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    andOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val andSeqOp = AndSeq(
      NamedElementPlaceholder("a") ::
      NamedElementPlaceholder("b") :: Nil
    )
    andSeqOp.validateAndRemoveVariablePlaceholders(context) should equal(Failure(ElementNotFoundException("a")))
    val orOp = Or(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    orOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val notOp = Not(
      NamedElementPlaceholder("a")
    )
    notOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil)
    andOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      And(
        Variable("a"),
        Variable("b")
      )
    )
    andSeqOp.validateAndRemoveVariablePlaceholders(context).get should equal(
      AndSeq(
        Variable("a") ::
        Variable("b") :: Nil
      )
    )
    orOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Or(
        Variable("a"),
        Variable("b")
      )
    )
    notOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Not(
        Variable("a")
      )
    )
  }

  "Comparison operation" should "be validated correctly" in {
    val context = ValidationContext()
    val eqOp = Equals(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    eqOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val eqGtOp = GreaterOrEqualsThan(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    eqGtOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val gtOp = GreaterThan(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    gtOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val eqLsOp = LessOrEqualsThan(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    eqLsOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val lsOp = LessThan(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    lsOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val notEqOp = NotEquals(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    notEqOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val betweenOp = Between(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b"),
      NamedElementPlaceholder("c")
    )
    betweenOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: VariableDefinition("c", None) :: Nil)
    eqOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Equals(
        Variable("a"),
        Variable("b")
      )
    )
    eqGtOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      GreaterOrEqualsThan(
        Variable("a"),
        Variable("b")
      )
    )
    gtOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      GreaterThan(
        Variable("a"),
        Variable("b")
      )
    )
    eqLsOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      LessOrEqualsThan(
        Variable("a"),
        Variable("b")
      )
    )
    lsOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      LessThan(
        Variable("a"),
        Variable("b")
      )
    )
    notEqOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      NotEquals(
        Variable("a"),
        Variable("b")
      )
    )
    betweenOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Between(
        Variable("a"),
        Variable("b"),
        Variable("c")
      )
    )
  }

  "Arithmetic operations" should "be validated correctly" in {
    val context = ValidationContext()
    val addOp = Add(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    addOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val divOp = Divide(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    divOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val mulOp = Multiply(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    mulOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val subOp = Substract(
      NamedElementPlaceholder("a"),
      NamedElementPlaceholder("b")
    )
    subOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))
    val unMinOp = UnaryMinus(NamedElementPlaceholder("a"))
    unMinOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    context.addVariables(VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil)
    addOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Add(
        Variable("a"),
        Variable("b")
      )
    )
    divOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Divide(
        Variable("a"),
        Variable("b")
      )
    )
    mulOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Multiply(
        Variable("a"),
        Variable("b")
      )
    )
    subOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Substract(
        Variable("a"),
        Variable("b")
      )
    )
    unMinOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      UnaryMinus(Variable("a"))
    )
  }

  "Relational operations" should "be validated correctly" in {
    val context = ValidationContext()
    val inListOp = InList(
      NamedElementPlaceholder("a"),
      ListInitialization(
        List(IntLiteral(1), IntLiteral(2), IntLiteral(3))
      ))
    inListOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    val inSubQueryOp = InSubQuery(
      NamedElementPlaceholder("a"),
      AnonymousConceptDefinition.builder(
        Attribute("id", None, Nil) :: Nil,
        ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), StringLiteral("footwear")))
        .build
    )
    inSubQueryOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    val allOp = All(
      GreaterThan(
        NamedElementPlaceholder("a"),
        AnonymousConceptDefinition.builder(
          Attribute("avgPrice", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
          .build
      )
    )
    allOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    val anyOp =Any(
      GreaterOrEqualsThan(
        NamedElementPlaceholder("a"),
        AnonymousConceptDefinition.builder(
          Attribute("avgPrice", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
          .build
      )
    )
    anyOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("a")))

    context.addVariable(VariableDefinition("a", None))
    inListOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      InList(
        Variable("a"),
        ListInitialization(
          List(IntLiteral(1), IntLiteral(2), IntLiteral(3))
      ))
    )
    inSubQueryOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("products")))
    allOp.validateAndRemoveVariablePlaceholders(context) should equal(Failure(ElementNotFoundException("products")))
    anyOp.validateAndRemoveVariablePlaceholders(context) should equal(Failure(ElementNotFoundException("products")))

    val existsOp = Exists(
      AnonymousConceptDefinition.builder(
        Attribute("id", None, Nil) :: Nil,
        ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(NamedElementPlaceholder("a"), ConceptAttribute("c2" :: Nil, "id")))
        .build
    )
    existsOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("products")))

    val uniqueOp = Unique(
      AnonymousConceptDefinition.builder(
        Attribute("id", None, Nil) :: Nil,
        ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(NamedElementPlaceholder("a"), ConceptAttribute("c2" :: Nil, "category")))
        .build
    )
    uniqueOp.validateAndRemoveVariablePlaceholders(context) should equal (Failure(ElementNotFoundException("products")))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "products",
      Attribute("id", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("category", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    inSubQueryOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      InSubQuery(
        Variable("a"),
        AnonymousConceptDefinition.builder(
          Attribute("id", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), StringLiteral("footwear")))
          .build
      )
    )

    allOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      All(
        GreaterThan(
          Variable("a"),
          AnonymousConceptDefinition.builder(
            Attribute("avgPrice", None, Nil) :: Nil,
            ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
            .build
        )
      )
    )

    anyOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Any(
        GreaterOrEqualsThan(
          Variable("a"),
          AnonymousConceptDefinition.builder(
            Attribute("avgPrice", None, Nil) :: Nil,
            ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
            .build
        )
      )
    )

    existsOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Exists(
        AnonymousConceptDefinition.builder(
          Attribute("id", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(Variable("a"), ConceptAttribute("c2" :: Nil, "id")))
          .build
      )
    )

    uniqueOp.validateAndRemoveVariablePlaceholders(context).get should equal (
      Unique(
        AnonymousConceptDefinition.builder(
          Attribute("id", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(Variable("a"), ConceptAttribute("c2" :: Nil, "category")))
          .build
      )
    )

  }


}
