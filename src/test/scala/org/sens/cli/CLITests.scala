package org.sens.cli

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.cli.MainApp.validateQuery
import org.sens.converter.rel.ConceptSqlComposer
import org.sens.parser.SensParser

class CLITests extends AnyFlatSpec with Matchers {

  val program =
    """
      |datasource order (
      |  id,
      |  customer_id,
      |  order_date,
      |  planned_delivery_date,
      |  delivery_date,
      |  price_total,
      |  status
      |) from CSV file "orders.csv";
      |
      |datasource customer (
      |  id,
      |  name,
      |  email,
      |  status,
      |  plan_code
      |) from CSV file "customers.csv";
      |
      |concept premiumCustomer is customer where status = "ACTIVE" and plan_code = "PREMIUM";
      |
      |concept lateOrder is order where status = "DELIVERED" and delivery_date > planned_delivery_date;
      |
      |@Materialized (type = "Table")
      |concept premiumCustomersLateOrdersTotal (
      |  orderDate = o.order_date,
      |  ordersCount = count(o.id),
      |  priceTotal = sum(o.price_total)
      |) from lateOrder o, premiumCustomer c (id = o.customer_id)
      |group by orderDate;
      |
      |@Materialized (type = "Table")
      |concept cube orderMetrics
      |metrics (ordersCount = count(), priceSum = sum(price_total))
      |dimensions (order_date, status)
      |from order;
      |
      |concept cube customerOrderMetrics is orderMetrics
      |with dimensions customer_id, planned_delivery_date;
      |""".stripMargin

  "Program" should "be parsed and validated correctly" in {
    val conceptParser = new SensParser
    val parsingResults = MainApp.parseDataModel(program, conceptParser)
    parsingResults.isRight should be (true)
    val context = parsingResults.right.get
    context.getConcepts.size should equal (7)
  }

  "Program" should "be materialized correctly" in {
    val conceptParser = new SensParser
    val parsingResults = MainApp.parseDataModel(program, conceptParser)
    val context = parsingResults.right.get
    val sqlCode = MainApp.materialize(context)
    sqlCode should equal(
      """DROP TABLE IF EXISTS `orderMetrics`;
        |CREATE TABLE `orderMetrics` AS
        |SELECT COUNT(*) AS `ordersCount`, SUM(`order`.`price_total`) AS `priceSum`, `order`.`order_date` AS `order_date`, `order`.`status` AS `status`
        |FROM `orders.csv` AS `order`
        |GROUP BY `order_date`, `status`;
        |DROP TABLE IF EXISTS `premiumCustomersLateOrdersTotal`;
        |CREATE TABLE `premiumCustomersLateOrdersTotal` AS
        |WITH `lateOrder` AS (SELECT `order`.`id` AS `id`, `order`.`customer_id` AS `customer_id`, `order`.`order_date` AS `order_date`, `order`.`planned_delivery_date` AS `planned_delivery_date`, `order`.`delivery_date` AS `delivery_date`, `order`.`price_total` AS `price_total`, `order`.`status` AS `status`
        |FROM `orders.csv` AS `order`
        |WHERE `order`.`status` = 'DELIVERED' AND `order`.`delivery_date` > `order`.`planned_delivery_date`), `premiumCustomer` AS (SELECT `customer`.`id` AS `id`, `customer`.`name` AS `name`, `customer`.`email` AS `email`, `customer`.`status` AS `status`, `customer`.`plan_code` AS `plan_code`
        |FROM `customers.csv` AS `customer`
        |WHERE `customer`.`status` = 'ACTIVE' AND `customer`.`plan_code` = 'PREMIUM') (SELECT `o`.`order_date` AS `orderDate`, COUNT(`o`.`id`) AS `ordersCount`, SUM(`o`.`price_total`) AS `priceTotal`
        |FROM `lateOrder` AS `o`
        |INNER JOIN `premiumCustomer` AS `c` ON `c`.`id` = `o`.`customer_id`
        |GROUP BY `orderDate`);""".stripMargin)
  }

  "Query" should "be parsed and validated correctly" in {
    val conceptParser = new SensParser
    val parsingResults = MainApp.parseDataModel(program, conceptParser)
    val context = parsingResults.right.get
    val composer = ConceptSqlComposer.create(context)

    val queryCode = "(id) from lateOrder(customer_id = \"123\")"
    val querySql = MainApp.queryToSql(queryCode, context, conceptParser, composer)

    querySql.right.get should equal (
      """WITH `lateOrder` AS (SELECT `order`.`id` AS `id`, `order`.`customer_id` AS `customer_id`, `order`.`order_date` AS `order_date`, `order`.`planned_delivery_date` AS `planned_delivery_date`, `order`.`delivery_date` AS `delivery_date`, `order`.`price_total` AS `price_total`, `order`.`status` AS `status`
        |FROM `orders.csv` AS `order`
        |WHERE `order`.`status` = 'DELIVERED' AND `order`.`delivery_date` > `order`.`planned_delivery_date`) (SELECT `lateOrder`.`id` AS `id`
        |FROM `lateOrder` AS `lateOrder`
        |WHERE `lateOrder`.`customer_id` = '123')""".stripMargin)

    val queryCode1 = "(priceSum) from customerOrderMetrics(customer_id = \"123\")"
    val querySql1 = MainApp.queryToSql(queryCode1, context, conceptParser, composer)
    querySql1.right.get should equal(
      """WITH `customerOrderMetrics` AS (SELECT COUNT(*) AS `ordersCount`, SUM(`order`.`price_total`) AS `priceSum`, `order`.`order_date` AS `order_date`, `order`.`status` AS `status`, `order`.`customer_id` AS `customer_id`, `order`.`planned_delivery_date` AS `planned_delivery_date`
        |FROM `orders.csv` AS `order`
        |GROUP BY `order_date`, `status`, `customer_id`, `planned_delivery_date`) (SELECT `customerOrderMetrics`.`priceSum` AS `priceSum`
        |FROM `customerOrderMetrics` AS `customerOrderMetrics`
        |WHERE `customerOrderMetrics`.`customer_id` = '123')""".stripMargin)
  }

  val genericProgram =
    """
      |datasource campaign_meta_ads(
      |  campaign_id,
      |  date,
      |  impressions,
      |  clicks,
      |  conversions,
      |  cost
      |) from CSV file "ppc_meta.csv";
      |
      |datasource campaign_google_ads(
      |  campaign_id,
      |  date,
      |  impressions,
      |  clicks,
      |  cost
      |) from CSV file "ppc_google.csv";
      |
      |concept adCampaignAggregated[source](
      |  impressions = sum(s.impressions),
      |  clicks = sum(s.clicks),
      |  cost = sum(s.cost),
      |  date = s.date
      |) from $source s
      |group by date;
      |
      |concept adStats[source] is adCampaignAggregated[source: source] s with
      |  cpc = s.cost / s.clicks,
      |  ctr = (s.clicks / s.impressions) * 100;
      |
      |@Materialized (type = "Table")
      |concept adCampaignsComparison(
      |  costGoogle = g.cost,
      |  costMeta = m.cost,
      |  cpcGoogle = g.cpc,
      |  cpcMeta = m.cpc,
      |  ctrGoogle = g.ctr,
      |  ctrMeta = m.ctr,
      |  date = g.date
      |) from adStats[source: "campaign_meta_ads"] m, adStats[source: "campaign_google_ads"] g (date = m.date);
      |""".stripMargin

  "Generic Program" should "be materialized correctly" in {
    val conceptParser = new SensParser

    val parsingResults = MainApp.parseDataModel(genericProgram, conceptParser)
    parsingResults.isRight should be(true)
    val context = parsingResults.right.get
    context.getConcepts.size should equal(5)

    val sqlCode = MainApp.materialize(context)
    sqlCode should equal(
      """DROP TABLE IF EXISTS `adCampaignsComparison`;
        |CREATE TABLE `adCampaignsComparison` AS
        |WITH `_generic_adCampaignAggregated_campaign_meta_ads` AS (SELECT SUM(`s`.`impressions`) AS `impressions`, SUM(`s`.`clicks`) AS `clicks`, SUM(`s`.`cost`) AS `cost`, `s`.`date` AS `date`
        |FROM `ppc_meta.csv` AS `s`
        |GROUP BY `date`), `_generic_adCampaignAggregated_campaign_google_ads` AS (SELECT SUM(`s`.`impressions`) AS `impressions`, SUM(`s`.`clicks`) AS `clicks`, SUM(`s`.`cost`) AS `cost`, `s`.`date` AS `date`
        |FROM `ppc_google.csv` AS `s`
        |GROUP BY `date`), `_generic_adStats_campaign_meta_ads` AS (SELECT `s`.`impressions` AS `impressions`, `s`.`clicks` AS `clicks`, `s`.`cost` AS `cost`, `s`.`date` AS `date`, `s`.`cost` / `s`.`clicks` AS `cpc`, `s`.`clicks` / `s`.`impressions` * 100 AS `ctr`
        |FROM `_generic_adCampaignAggregated_campaign_meta_ads` AS `s`), `_generic_adStats_campaign_google_ads` AS (SELECT `s`.`impressions` AS `impressions`, `s`.`clicks` AS `clicks`, `s`.`cost` AS `cost`, `s`.`date` AS `date`, `s`.`cost` / `s`.`clicks` AS `cpc`, `s`.`clicks` / `s`.`impressions` * 100 AS `ctr`
        |FROM `_generic_adCampaignAggregated_campaign_google_ads` AS `s`) (SELECT `g`.`cost` AS `costGoogle`, `m`.`cost` AS `costMeta`, `g`.`cpc` AS `cpcGoogle`, `m`.`cpc` AS `cpcMeta`, `g`.`ctr` AS `ctrGoogle`, `m`.`ctr` AS `ctrMeta`, `g`.`date` AS `date`
        |FROM `_generic_adStats_campaign_meta_ads` AS `m`
        |INNER JOIN `_generic_adStats_campaign_google_ads` AS `g` ON `g`.`date` = `m`.`date`);""".stripMargin)
  }

  "Query to generic concept" should "be parsed and validated correctly" in {
    val conceptParser = new SensParser
    val parsingResults = MainApp.parseDataModel(genericProgram, conceptParser)
    val context = parsingResults.right.get
    val composer = ConceptSqlComposer.create(context)

    val queryCode = "(cpc, ctr) from adStats[source: \"campaign_meta_ads\"](date = \"2024-01-01\")"
    val querySql = MainApp.queryToSql(queryCode, context, conceptParser, composer)

    querySql.right.get should equal(
      """WITH `_generic_adCampaignAggregated_campaign_meta_ads` AS (SELECT SUM(`s`.`impressions`) AS `impressions`, SUM(`s`.`clicks`) AS `clicks`, SUM(`s`.`cost`) AS `cost`, `s`.`date` AS `date`
        |FROM `ppc_meta.csv` AS `s`
        |GROUP BY `date`), `_generic_adStats_campaign_meta_ads` AS (SELECT `s`.`impressions` AS `impressions`, `s`.`clicks` AS `clicks`, `s`.`cost` AS `cost`, `s`.`date` AS `date`, `s`.`cost` / `s`.`clicks` AS `cpc`, `s`.`clicks` / `s`.`impressions` * 100 AS `ctr`
        |FROM `_generic_adCampaignAggregated_campaign_meta_ads` AS `s`) (SELECT `_generic_adStats_campaign_meta_ads`.`cpc` AS `cpc`, `_generic_adStats_campaign_meta_ads`.`ctr` AS `ctr`
        |FROM `_generic_adStats_campaign_meta_ads` AS `_generic_adStats_campaign_meta_ads`
        |WHERE `_generic_adStats_campaign_meta_ads`.`date` = '2024-01-01')""".stripMargin)
  }

  val conceptAttributesProgram =
    """
      |datasource campaign_ads(
      |  campaign_id,
      |  date,
      |  impressions,
      |  clicks,
      |  cost
      |) from CSV file "ppc.csv";
      |
      |concept attributes adCampaignMetrics [sourceName] (
      |  cost = sum(s.cost),
      |  clicks = sum(s.clicks),
      |  impressions = sum(s.impressions),
      |  cpc = sum(s.cost) / sum(s.clicks),
      |  ctr = 100 * (sum(s.clicks) / sum(s.impressions))
      |) from $sourceName s;
      |
      |@Materialized (type = "Table")
      |concept adStatsByCampaign (
      |  adCampaignMetrics[sourceName: "campaign_ads", s: "ca"],
      |  campaign_id,
      |  date
      |) from campaign_ads ca
      |group by campaign_id, date;
      |
      |@Materialized (type = "Table")
      |concept adStatsDaily (
      |  adCampaignMetrics[sourceName: "adStatsByCampaign", s: "ca"],
      |  date
      |) from adStatsByCampaign ca
      |group by date;
      |""".stripMargin

  "Concept Attributes Program" should "be materialized correctly" in {
    val conceptParser = new SensParser

    val parsingResults = MainApp.parseDataModel(conceptAttributesProgram, conceptParser)
    parsingResults.isRight should be(true)
    val context = parsingResults.right.get
    context.getConcepts.size should equal(4)

    val sqlCode = MainApp.materialize(context)
    sqlCode should equal(
      """DROP TABLE IF EXISTS `adStatsByCampaign`;
        |CREATE TABLE `adStatsByCampaign` AS
        |SELECT SUM(`ca`.`cost`) AS `cost`, SUM(`ca`.`clicks`) AS `clicks`, SUM(`ca`.`impressions`) AS `impressions`, SUM(`ca`.`cost`) / SUM(`ca`.`clicks`) AS `cpc`, 100 * (SUM(`ca`.`clicks`) / SUM(`ca`.`impressions`)) AS `ctr`, `ca`.`campaign_id` AS `campaign_id`, `ca`.`date` AS `date`
        |FROM `ppc.csv` AS `ca`
        |GROUP BY `campaign_id`, `date`;
        |DROP TABLE IF EXISTS `adStatsDaily`;
        |CREATE TABLE `adStatsDaily` AS
        |SELECT SUM(`ca`.`cost`) AS `cost`, SUM(`ca`.`clicks`) AS `clicks`, SUM(`ca`.`impressions`) AS `impressions`, SUM(`ca`.`cost`) / SUM(`ca`.`clicks`) AS `cpc`, 100 * (SUM(`ca`.`clicks`) / SUM(`ca`.`impressions`)) AS `ctr`, `ca`.`date` AS `date`
        |FROM `adStatsByCampaign` AS `ca`
        |GROUP BY `date`;""".stripMargin)
  }
}
