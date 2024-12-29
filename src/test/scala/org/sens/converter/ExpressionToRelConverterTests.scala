package org.sens.converter

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptToRelConverter
import org.sens.core.concept.{Annotation, Attribute, Concept, DataSourceConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.{CollectionItem, ConceptAttribute, ConceptObject, FunctionCall, If, ListInitialization, MapInitialization, Switch, WindowFunction, WindowFunctions}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.core.statement.{ConceptDefinition, FunctionDefinition, NOP}
import org.sens.parser.ValidationContext

class ExpressionToRelConverterTests extends AnyFlatSpec with Matchers {

  "Arithmetic operations" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(
          Multiply(
            UnaryMinus(ConceptAttribute("cb" :: Nil, "attr2")),
            IntLiteral(2)),
          Divide(
            Substract(ConceptAttribute("cb" :: Nil, "attr3"), FloatLiteral(1.0)),
            IntLiteral(3)
          ))),
          Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, - `attr2` * 2 + (`attr3` - 1.0) / 3 AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0"
    )
  }

  "Comparison operations" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) ::
        Attribute("attr6", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")),
          Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(0)),
        And(
          LessThan(ConceptAttribute("cb" :: Nil, "attr2"), IntLiteral(0)),
          And(
            GreaterOrEqualsThan(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(0)),
            And(
              LessOrEqualsThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
              And(
                Equals(ConceptAttribute("cb" :: Nil, "attr5"), IntLiteral(0)),
                And(
                  NotEquals(ConceptAttribute("cb" :: Nil, "attr6"), IntLiteral(0)),
                  Between(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))
                )
              ))))))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef.inferAttributeExpressions(context).get)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, `attr2` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr5` = 0 AND `attr1` > 0 AND (`attr2` < 0 AND `attr3` >= 0) AND (`attr4` <= 0 AND `attr6` <> 0 AND (`attr1` >= `attr2` AND `attr1` <= `attr3`))"
    )
  }

  "Logic operations" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")),
          Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(1)),
        Or(
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), IntLiteral(1)),
          Not(ConceptAttribute("cb" :: Nil, "attr3"))
        )
      ))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1` AS `id`, `attr2` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr1` = 1 AND (`attr2` = 1 OR NOT `attr3`)"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")),
          Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(AndSeq(
        Equals(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(1)) ::
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), IntLiteral(2)) ::
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(3)) :: Nil
      ))
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal(
      "SELECT `attr1` AS `id`, `attr2` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr1` = 1 AND `attr2` = 2 AND `attr3` = 3"
    )
  }

  "Literals" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr5", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr6", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr7", None, Annotation.OPTIONAL :: Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val1", Some(BooleanLiteral(true)), Nil) ::
        Attribute("val2", Some(BooleanLiteral(false)), Nil) ::
        Attribute("val3", Some(NullLiteral()), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(1)),
        And(
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), FloatLiteral(1)),
          And(
            Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("1")),
            And(
              Equals(ConceptAttribute("cb" :: Nil, "attr4"), NullLiteral()),
              And(
                NotEquals(ConceptAttribute("cb" :: Nil, "attr5"), NullLiteral()),
                And(
                  Equals(NullLiteral(), ConceptAttribute("cb" :: Nil, "attr6")),
                  NotEquals(NullLiteral(), ConceptAttribute("cb" :: Nil, "attr7"))
                )
              )
        )))))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, TRUE AS `val1`, FALSE AS `val2`, NULL AS `val3`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr1` = 1 AND (`attr2` = 1.0 AND `attr3` = '1') AND (`attr4` IS NULL AND `attr6` IS NULL AND (`attr5` IS NOT NULL AND `attr7` IS NOT NULL))"
    )
  }

  "Array literals" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("arrayLiteral", Some(ListLiteral(
          IntLiteral(1) ::
            IntLiteral(2) ::
            IntLiteral(3) :: Nil
        )), Nil) ::
        Attribute("arrayVal", Some(ListInitialization(
          ConceptAttribute("cb" :: Nil, "attr2") ::
            ConceptAttribute("cb" :: Nil, "attr3") ::
            StringLiteral("someVal") :: Nil
        )), Nil) ::
        Attribute("mapLiteral", Some(MapLiteral(Map(
          StringLiteral("key1") -> BooleanLiteral(true),
          StringLiteral("key2") -> BooleanLiteral(false)
        ))), Nil) ::
        Attribute("mapVal", Some(MapInitialization(Map(
         StringLiteral("attr2column") -> ConceptAttribute("cb" :: Nil, "attr2"),
          StringLiteral("attr3column") -> ConceptAttribute("cb" :: Nil, "attr3")
        ))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, ARRAY[1, 2, 3] AS `arrayLiteral`, ARRAY[`attr2`, `attr3`, 'someVal'] AS `arrayVal`, MAP['key1', TRUE, 'key2', FALSE] AS `mapLiteral`, MAP['attr2column', `attr2`, 'attr3column', `attr3`] AS `mapVal`\n" +
        "FROM `vs`.`conceptB`"
    )
  }

  "Collection item" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val1", Some(CollectionItem(
          ConceptAttribute("cb" :: Nil, "attr2"),
          IntLiteral(0) :: Nil
        )), Nil) ::
        Attribute("val2", Some(CollectionItem(
          ConceptAttribute("cb" :: Nil, "attr2"),
          IntLiteral(0) ::
          StringLiteral("key") :: Nil
        )), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, `attr2`[0] AS `val1`, `attr2`[0]['key'] AS `val2`\n" +
        "FROM `vs`.`conceptB`"
    )
  }

  "Functions" should "be converted to relational algebra correctly and propagated to DB" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    context.addFunction(FunctionDefinition(
      "myFunc",
      "arg1" :: "arg2" :: Nil,
      NOP()
    ))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(FunctionCall(
          FunctionReference("myFunc"),
          ConceptAttribute("cb" :: Nil, "attr2") :: ConceptAttribute("cb" :: Nil, "attr3") :: Nil)),
          Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, `myFunc`(`attr2`, `attr3`) AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `myFunc`(`attr2`, `attr3`) = 1 AND `attr4` > 0"
    )
  }

  "Switch and If calls" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Switch(Map(
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), StringLiteral("A")) -> IntLiteral(1),
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), StringLiteral("B")) -> IntLiteral(2),
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), StringLiteral("C")) -> IntLiteral(3)
        ),
          IntLiteral(0)
        )),
          Nil) ::
        Attribute("status", Some(If(
          NotEquals(ConceptAttribute("cb" :: Nil, "attr3"), NullLiteral()),
          ConceptAttribute("cb" :: Nil, "attr3"),
          StringLiteral("DefaultValue")
        )), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, CASE WHEN `attr2` = 'A' THEN 1 WHEN `attr2` = 'B' THEN 2 WHEN `attr2` = 'C' THEN 3 ELSE 0 END AS `val`, CASE WHEN `attr3` IS NOT NULL THEN `attr3` ELSE 'DefaultValue' END AS `status`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0"
    )
  }

  "Concept objects" should "collect attributes and be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
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

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT ROW(`attr1`, `attr2`, `attr3`, `attr4`) AS `obj`\n" +
        "FROM `vs`.`conceptB`"
    )
  }

  "Standard Sens functions" should "be mapped to SQL operators and converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr3", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr4", None, Annotation.OPTIONAL :: Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val1", Some(FunctionCall(
          FunctionReference("concat"),
          ConceptAttribute("cb" :: Nil, "attr2") :: ConceptAttribute("cb" :: Nil, "attr3") :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)),
          Nil) ::
        Attribute("val2", Some(FunctionCall(
          FunctionReference("like"),
          ConceptAttribute("cb" :: Nil, "attr2") :: StringLiteral("%pattern") :: Nil)),
          Nil) ::
        Attribute("val3", Some(FunctionCall(
          FunctionReference("length"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val4", Some(FunctionCall(
          FunctionReference("upper"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val5", Some(FunctionCall(
          FunctionReference("lower"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val6", Some(FunctionCall(
          FunctionReference("substring"),
          ConceptAttribute("cb" :: Nil, "attr2") :: IntLiteral(3) :: Nil)),
          Nil) ::
        Attribute("val7", Some(FunctionCall(
          FunctionReference("substring"),
          ConceptAttribute("cb" :: Nil, "attr2") :: IntLiteral(3) :: IntLiteral(3) :: Nil)),
          Nil) ::
        Attribute("val8", Some(FunctionCall(
          FunctionReference("replace"),
          ConceptAttribute("cb" :: Nil, "attr2") :: StringLiteral("placeholder") :: StringLiteral("value") :: Nil)),
          Nil) ::
        Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, " +
        "`attr2` || `attr3` || `attr4` AS `val1`, " +
        "`attr2` LIKE '%pattern' AS `val2`, " +
        "CHAR_LENGTH(`attr2`) AS `val3`, " +
        "UPPER(`attr2`) AS `val4`, " +
        "LOWER(`attr2`) AS `val5`, " +
        "SUBSTRING(`attr2` FROM 3) AS `val6`, " +
        "SUBSTRING(`attr2` FROM 3 FOR 3) AS `val7`, " +
        "REPLACE(`attr2`, 'placeholder', 'value') AS `val8`\n" +
        "FROM `vs`.`conceptB`"
    )

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val1", Some(FunctionCall(
          FunctionReference("mod"),
          ConceptAttribute("cb" :: Nil, "attr2") :: IntLiteral(2) :: Nil)),
          Nil) ::
        Attribute("val2", Some(FunctionCall(
          FunctionReference("round"),
          ConceptAttribute("cb" :: Nil, "attr2") :: IntLiteral(3) :: Nil)),
          Nil) ::
        Attribute("val3", Some(FunctionCall(
          FunctionReference("round"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val4", Some(FunctionCall(
          FunctionReference("ceil"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val5", Some(FunctionCall(
          FunctionReference("floor"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val6", Some(FunctionCall(
          FunctionReference("trunc"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val7", Some(FunctionCall(
          FunctionReference("abs"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Attribute("val8", Some(FunctionCall(
          FunctionReference("sqrt"),
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)),
          Nil) ::
        Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1` AS `id`, " +
        "MOD(`attr2`, 2) AS `val1`, " +
        "ROUND(`attr2`, 3) AS `val2`, " +
        "ROUND(`attr2`) AS `val3`, " +
        "CEIL(`attr2`) AS `val4`, " +
        "FLOOR(`attr2`) AS `val5`, " +
        "TRUNCATE(`attr2`) AS `val6`, " +
        "ABS(`attr2`) AS `val7`, " +
        "SQRT(`attr2`) AS `val8`\n" +
        "FROM `vs`.`conceptB`"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val1", Some(FunctionCall(
          FunctionReference("coalesce"),
          ConceptAttribute("cb" :: Nil, "attr2") :: ConceptAttribute("cb" :: Nil, "attr3") :: ConceptAttribute("cb" :: Nil, "attr4") :: StringLiteral("N/A") :: Nil)),
          Nil) ::
        Attribute("val2", Some(FunctionCall(
          FunctionReference("cast"),
          ConceptAttribute("cb" :: Nil, "attr3") :: BasicTypeLiteral(SensBasicTypes.INT_TYPE) :: Nil)),
          Nil) ::
        Attribute("val3", Some(FunctionCall(
          FunctionReference("cast"),
          ConceptAttribute("cb" :: Nil, "attr4") :: BasicTypeLiteral(SensBasicTypes.STRING_TYPE)  :: IntLiteral(8) :: Nil)),
          Nil) ::
        Attribute("val4", Some(FunctionCall(
          FunctionReference("current_time"),
          Nil)),
          Nil) ::
        Attribute("val5", Some(FunctionCall(
          FunctionReference("current_date"),
          Nil)),
          Nil) ::
        Attribute("val6", Some(FunctionCall(
          FunctionReference("current_timestamp"),
          Nil)),
          Nil) ::
        Attribute("val7", Some(
          Add(
            ConceptAttribute("cb" :: Nil, "attr2"),
            TimeIntervalLiteral(1, TimeIntervalLiteral.MONTH)
          )),
          Nil) ::
        Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `attr1` AS `id`, " +
        "COALESCE(`attr2`, `attr3`, `attr4`, 'N/A') AS `val1`, " +
        "CAST(`attr3` AS INTEGER) AS `val2`, " +
        "CAST(`attr4` AS VARCHAR CHARACTER SET `ISO-8859-1`) AS `val3`, " +
        "CURRENT_TIME AS `val4`, " +
        "CURRENT_DATE AS `val5`, " +
        "CURRENT_TIMESTAMP AS `val6`, " +
        "`attr2` + INTERVAL '1' MONTH(0) AS `val7`\n" +
        "FROM `vs`.`conceptB`"
    )
  }

  "Relational Sens operators" should "be mapped to SQL operators and converted to relational algebra correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "products",
      Attribute("id", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("category", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "categories",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conceptConverter = ConceptToRelConverter.create(context)

    val conDef1 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(InList(
      ConceptAttribute("p" :: Nil, "category"),
      ListInitialization(
        List(IntLiteral(1), IntLiteral(2), IntLiteral(3))
      ))
    ).build
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal(
      "SELECT *\n" +
        "FROM `vs`.`products`\n" +
        "WHERE `category` IN (1, 2, 3)"
    )

    val conDef2 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(InSubQuery(
      ConceptAttribute("p" :: Nil, "category"),
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
        .build)
    ).build
    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal(
      "SELECT *\n" +
        "FROM `vs`.`products`\n" +
        "WHERE `category` IN (" +
        "SELECT `id`\n" +
        "FROM `vs`.`categories`\n" +
        "WHERE `parentCategory` = 1)"
    )

/*
    val conDef3 =Concept.builder(
      "someCategories",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
      ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil
    ).attributeDependencies(
      Exists(
        AnonymousConceptDefinition.builder(
          Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("p" :: Nil, "category")))
          .build
      )
    ).build
    val conceptRel3 = conceptConverter.toRel(conDef3)
    toSql(conceptRel3) should equal(
      "SELECT `id`\n" +
        "FROM `vs`.`categories` AS `t`\n" +
        "WHERE EXISTS (" +
        "SELECT `id`\n" +
        "FROM `vs`.`products`\n" +
        "WHERE `t`.`id` = `category`)"
    )

    val conDef4 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(Any(GreaterThan(
      ConceptAttribute("p" :: Nil, "category"),
      AnonymousConceptDefinition.builder(
        Attribute("price", Some(ConceptAttribute("p1" :: Nil, "price")), Nil) :: Nil,
        ParentConcept(ConceptReference("products"), Some("p1"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("p1" :: Nil, "category"), IntLiteral(1)))
        .build))
    ).build
    val conceptRel4 = conceptConverter.toRel(conDef3)
    toSql(conceptRel4) should equal(
      "SELECT *\n" +
        "FROM `vs`.`products`\n" +
        "WHERE `price` > ANY (" +
        "SELECT `price`\n" +
        "FROM `vs`.`products`" +
        "WHERE `category` = 1)"
    )*/
  }

  "Window functions" should "be mapped to SQL operators and converted to relational algebra correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "products",
      Attribute("id", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conceptConverter = ConceptToRelConverter.create(context)

    val conDef = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("priceAvg", Some(
          WindowFunction(
            WindowFunctions.AVG,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            Nil,
            Nil,
            (None, None),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumByCategory", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Nil,
            (None, None),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumByDate", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (None, None),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumLimited1", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (Some(IntLiteral(1)), None),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumLimited2", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (None, Some(IntLiteral(2))),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumLimited3", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (Some(IntLiteral(1)), Some(IntLiteral(2))),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumLimited4", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (None, None),
            (Some(IntLiteral(1)), None)
          )
        ), Nil) ::
        Attribute("priceSumLimited5", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (None, None),
            (None, Some(IntLiteral(2)))
          )
        ), Nil) ::
        Attribute("priceSumLimited6", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Order(ConceptAttribute("p" :: Nil, "date"), Order.DESC) :: Nil,
            (None, None),
            (Some(IntLiteral(1)), Some(IntLiteral(2)))
          )
        ), Nil) ::
        Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).build()
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `id`, " +
        "AVG(`price`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `priceAvg`, " +
        "SUM(`price`) OVER (PARTITION BY `category` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `priceSumByCategory`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `priceSumByDate`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS `priceSumLimited1`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS `priceSumLimited2`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS `priceSumLimited3`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS `priceSumLimited4`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS `priceSumLimited5`, " +
        "SUM(`price`) OVER (PARTITION BY `category` ORDER BY `date` DESC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS `priceSumLimited6`\n" +
        "FROM `vs`.`products`"
    )

  }


  def toSql(relNode: RelNode): String = {
    val sqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val sqlNode = sqlConverter.visitRoot(relNode).asStatement()
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }

}
