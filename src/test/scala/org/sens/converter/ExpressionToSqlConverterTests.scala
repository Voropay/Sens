package org.sens.converter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.{ConceptToSqlConverter, ExpressionsToSqlConverter}
import org.sens.core.concept.{Annotation, Attribute, Concept, DataSourceConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.FunctionCall
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.concept._
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.relational._
import org.sens.core.expression._
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class ExpressionToSqlConverterTests extends AnyFlatSpec with Matchers {
  val context = ValidationContext()
  val expressionsConverter = ExpressionsToSqlConverter.create(context, null)

  "Literals" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(IntLiteral(1)) should equal ("1")
    expressionsConverter.sensExpressionToSql(FloatLiteral(1.1)) should equal ("1.1")
    expressionsConverter.sensExpressionToSql(BooleanLiteral(true)) should equal ("TRUE")
    expressionsConverter.sensExpressionToSql(StringLiteral("test")) should equal ("'test'")
    expressionsConverter.sensExpressionToSql(NullLiteral()) should equal ("NULL")
    expressionsConverter.sensExpressionToSql(ListLiteral(IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)) should equal ("ARRAY[1, 2, 3]")
    expressionsConverter.sensExpressionToSql(
      MapLiteral(Map(IntLiteral(1) -> StringLiteral("one"), IntLiteral(2) -> StringLiteral("two")))
    ) should equal ("MAP[1, 'one', 2, 'two']")
    expressionsConverter.sensExpressionToSql(TimeIntervalLiteral(1, TimeIntervalLiteral.MONTH)) should equal ("INTERVAL '1' MONTH(0)")

    expressionsConverter.sensExpressionToSql(BasicTypeLiteral(SensBasicTypes.INT_TYPE)) should equal ("INTEGER")
    expressionsConverter.sensExpressionToSql(BasicTypeLiteral(SensBasicTypes.FLOAT_TYPE)) should equal ("FLOAT")
    expressionsConverter.sensExpressionToSql(BasicTypeLiteral(SensBasicTypes.BOOLEAN_TYPE)) should equal ("BOOLEAN")
    expressionsConverter.sensExpressionToSql(BasicTypeLiteral(SensBasicTypes.STRING_TYPE)) should equal ("VARCHAR")
  }

  "attributes and objects" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(ConceptAttribute(Nil, "attr")) should equal ("`attr`")
    expressionsConverter.sensExpressionToSql(ConceptAttribute("c" :: Nil, "attr")) should equal ("`c`.`attr`")
    expressionsConverter.sensExpressionToSql(ConceptAttribute("a" :: "b" :: Nil, "attr")) should equal ("`a`.`b`.`attr`")

    val ctx = context.addFrame
    ctx.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("obj", Some(ConceptObject("cb")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build
    ctx.setCurrentConcept(conDef)
    val converter = ExpressionsToSqlConverter.create(ctx, null)
    converter.sensExpressionToSql(ConceptObject("cb")) should equal ("ROW(`cb`.`attr1`, `cb`.`attr2`, `cb`.`attr3`, `cb`.`attr4`)")
  }

  "Function calls" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(
      FunctionCall(
        FunctionReference("like"),
        ConceptAttribute(Nil, "attr") :: StringLiteral("%pattern") :: Nil)
    ) should equal ("`attr` LIKE '%pattern'")
    expressionsConverter.sensExpressionToSql(
      FunctionCall(
        FunctionReference("length"),
        ConceptAttribute(Nil, "attr") :: Nil)
    ) should equal ("CHAR_LENGTH(`attr`)")
    expressionsConverter.sensExpressionToSql(
      FunctionCall(
        FunctionReference("cast"),
        ConceptAttribute(Nil, "attr") :: BasicTypeLiteral(SensBasicTypes.INT_TYPE) :: Nil)
    ) should equal("CAST(`attr` AS INTEGER)")
    expressionsConverter.sensExpressionToSql(
      FunctionCall(
        FunctionReference("floor"),
        ConceptAttribute(Nil, "attr") :: StringLiteral("day") :: Nil)
    ) should equal("FLOOR(`attr` TO 'day')")
  }

  "Collection operations" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(
      ListInitialization(ConceptAttribute(Nil, "attr1") :: ConceptAttribute(Nil, "attr2") :: ConceptAttribute(Nil, "attr3") :: Nil)
    ) should equal("ARRAY[`attr1`, `attr2`, `attr3`]")
    expressionsConverter.sensExpressionToSql(
      MapInitialization(Map(ConceptAttribute(Nil, "attr1") -> StringLiteral("one"), ConceptAttribute(Nil, "attr2") -> StringLiteral("two")))
    ) should equal("MAP[`attr1`, 'one', `attr2`, 'two']")
    expressionsConverter.sensExpressionToSql(
      CollectionItem(
        ConceptAttribute(Nil, "attr"),
        IntLiteral(0) :: Nil
      )
    ) should equal ("`attr`[0]")
    expressionsConverter.sensExpressionToSql(
      CollectionItem(
        ConceptAttribute(Nil, "attr"),
        IntLiteral(0) ::
          StringLiteral("key") :: Nil
      )
    ) should equal ("`attr`[0]['key']")
  }

  "Logical operators" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(And(BooleanLiteral(true), BooleanLiteral(false))) should equal ("TRUE AND FALSE")
    expressionsConverter.sensExpressionToSql(AndSeq(BooleanLiteral(true) :: BooleanLiteral(false) :: Nil)) should equal ("TRUE AND FALSE")
    expressionsConverter.sensExpressionToSql(AndSeq(BooleanLiteral(true) :: BooleanLiteral(false) :: BooleanLiteral(true) :: Nil)) should equal ("TRUE AND (FALSE AND TRUE)")
    expressionsConverter.sensExpressionToSql(Or(BooleanLiteral(true), BooleanLiteral(false))) should equal ("TRUE OR FALSE")
    expressionsConverter.sensExpressionToSql(Not(BooleanLiteral(true))) should equal ("NOT TRUE")
  }

  "Arithmetical operators" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(Add(IntLiteral(1), IntLiteral(2))) should equal("1 + 2")
    expressionsConverter.sensExpressionToSql(Substract(IntLiteral(1), IntLiteral(2))) should equal("1 - 2")
    expressionsConverter.sensExpressionToSql(Multiply(IntLiteral(1), IntLiteral(2))) should equal("1 * 2")
    expressionsConverter.sensExpressionToSql(Divide(IntLiteral(1), IntLiteral(2))) should equal("1 / 2")
    expressionsConverter.sensExpressionToSql(UnaryMinus(IntLiteral(1))) should equal("- 1")
  }

  "Comparison operators" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(Equals(IntLiteral(1), IntLiteral(2))) should equal("1 = 2")
    expressionsConverter.sensExpressionToSql(Equals(ConceptAttribute(Nil, "a"), NullLiteral())) should equal("`a` IS NULL")
    expressionsConverter.sensExpressionToSql(Equals(NullLiteral(), ConceptAttribute(Nil, "a"))) should equal("`a` IS NULL")
    expressionsConverter.sensExpressionToSql(NotEquals(IntLiteral(1), IntLiteral(2))) should equal("1 <> 2")
    expressionsConverter.sensExpressionToSql(NotEquals(ConceptAttribute(Nil, "a"), NullLiteral())) should equal("`a` IS NOT NULL")
    expressionsConverter.sensExpressionToSql(NotEquals(NullLiteral(), ConceptAttribute(Nil, "a"))) should equal("`a` IS NOT NULL")
    expressionsConverter.sensExpressionToSql(GreaterThan(IntLiteral(1), IntLiteral(2))) should equal("1 > 2")
    expressionsConverter.sensExpressionToSql(GreaterOrEqualsThan(IntLiteral(1), IntLiteral(2))) should equal("1 >= 2")
    expressionsConverter.sensExpressionToSql(LessThan(IntLiteral(1), IntLiteral(2))) should equal("1 < 2")
    expressionsConverter.sensExpressionToSql(LessOrEqualsThan(IntLiteral(1), IntLiteral(2))) should equal("1 <= 2")
    expressionsConverter.sensExpressionToSql(Between(IntLiteral(1), IntLiteral(2), IntLiteral(3))) should equal("1 BETWEEN ASYMMETRIC 2 AND 3")
  }

  "Conditional expressions" should "be converted to SQL correctly" in {
    expressionsConverter.sensExpressionToSql(
      Switch(Map(
        Equals(ConceptAttribute(Nil, "attr2"), StringLiteral("A")) -> IntLiteral(1),
        Equals(ConceptAttribute(Nil, "attr2"), StringLiteral("B")) -> IntLiteral(2),
        Equals(ConceptAttribute(Nil, "attr2"), StringLiteral("C")) -> IntLiteral(3)
      ),
        IntLiteral(0)
      )
    ) should equal ("CASE WHEN `attr2` = 'A' THEN 1 WHEN `attr2` = 'B' THEN 2 WHEN `attr2` = 'C' THEN 3 ELSE 0 END")
    expressionsConverter.sensExpressionToSql(
      If(
        NotEquals(ConceptAttribute(Nil, "attr3"), NullLiteral()),
        ConceptAttribute(Nil, "attr3"),
        StringLiteral("DefaultValue")
      )
    ) should equal ("CASE WHEN `attr3` IS NOT NULL THEN `attr3` ELSE 'DefaultValue' END")
  }

  "Relational expressions" should ("be converted to SQL correctly") in {
    val ctx = context.addFrame
    ctx.addConcept(ConceptDefinition(DataSourceConcept(
      "categories",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Annotation.OPTIONAL :: Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    val conceptConverter = ConceptToSqlConverter.create(ctx)

    conceptConverter.expressionConverter.sensExpressionToSql(
      InList(
        ConceptAttribute(Nil, "attr"),
        ListInitialization(IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
      )
    ) should equal("`attr` IN (1, 2, 3)")

    conceptConverter.expressionConverter.sensExpressionToSql(
      InSubQuery(
        ConceptAttribute(Nil, "attr"),
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
          .build
      )
    ) should equal("`attr` IN (SELECT `c`.`id` AS `id`\nFROM `someFile` AS `c`\nWHERE `c`.`parentCategory` = 1)")

    conceptConverter.expressionConverter.sensExpressionToSql(
      All(GreaterThan(
        ConceptAttribute(Nil, "attr"),
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
          .build
      ))
    ) should equal("`attr` > ALL (SELECT `c`.`id` AS `id`\nFROM `someFile` AS `c`\nWHERE `c`.`parentCategory` = 1)")

    conceptConverter.expressionConverter.sensExpressionToSql(
      Any(GreaterThan(
        ConceptAttribute(Nil, "attr"),
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
          .build
      ))
    ) should equal("`attr` > SOME (SELECT `c`.`id` AS `id`\nFROM `someFile` AS `c`\nWHERE `c`.`parentCategory` = 1)")

    conceptConverter.expressionConverter.sensExpressionToSql(
      Exists(
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
          .build
      )
    ) should equal("EXISTS SELECT `c`.`id` AS `id`\nFROM `someFile` AS `c`\nWHERE `c`.`parentCategory` = 1")

    conceptConverter.expressionConverter.sensExpressionToSql(
      Unique(
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
          .build
      )
    ) should equal("UNIQUE SELECT `c`.`id` AS `id`\nFROM `someFile` AS `c`\nWHERE `c`.`parentCategory` = 1")
  }

  "Window function expressions" should ("be converted to SQL correctly") in {
    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.AVG,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        Nil,
        Nil,
        (None, None),
        (None, None)
      )
    ) should equal("AVG(`p`.`price`) OVER ()")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.SUM,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Nil,
        (None, None),
        (None, None)
      )
    ) should equal("SUM(`p`.`price`) OVER (PARTITION BY `p`.`category`)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.MAX,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (None, None)
      )
    ) should equal("MAX(`p`.`price`) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.MIN,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (Some(IntLiteral(1)), None),
        (None, None)
      )
    ) should equal("MIN(`p`.`price`) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC ROWS 1 PRECEDING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.ROW_NUMBER,
        Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, Some(IntLiteral(2))),
        (None, None)
      )
    ) should equal("ROW_NUMBER() OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.RANK,
        Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (Some(IntLiteral(1)), Some(IntLiteral(2))),
        (None, None)
      )
    ) should equal("RANK() OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.FIRST_VALUE,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (Some(IntLiteral(0)), Some(IntLiteral(2))),
        (None, None)
      )
    ) should equal("FIRST_VALUE(`p`.`price`) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.LAST_VALUE,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (Some(IntLiteral(1)), Some(IntLiteral(0))),
        (None, None)
      )
    ) should equal("LAST_VALUE(`p`.`price`) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.DENSE_RANK,
        Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (Some(IntLiteral(1)), None)
      )
    ) should equal("DENSE_RANK() OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC RANGE 1 PRECEDING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.CUME_DIST,
        Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (None, Some(IntLiteral(2)))
      )
    ) should equal("CUME_DIST() OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.LEAD,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (Some(IntLiteral(1)), Some(IntLiteral(2)))
      )
    ) should equal("LEAD(`p`.`price`, 1, NULL) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.LAG,
        ConceptAttribute("p" :: Nil, "price") :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (Some(IntLiteral(1)), Some(IntLiteral(0)))
      )
    ) should equal("LAG(`p`.`price`, 1, NULL) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.NTILE,
        IntLiteral(4) :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
        (None, None),
        (Some(IntLiteral(0)), Some(IntLiteral(2)))
      )
    ) should equal("NTILE(4) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date` DESC RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)")

    expressionsConverter.sensExpressionToSql(
      WindowFunction(
        WindowFunctions.NTH_VALUE,
        ConceptAttribute("p" :: Nil, "price") :: IntLiteral(2) :: Nil,
        ConceptAttribute("p" :: Nil, "category") :: Nil,
        Order(ConceptAttribute("p" :: Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (None, None)
      )
    ) should equal("NTH_VALUE(`p`.`price`, 2) OVER (PARTITION BY `p`.`category` ORDER BY `p`.`date`)")
  }

}
