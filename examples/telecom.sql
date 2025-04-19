DELETE FROM `callsMetricsUnion`
WHERE `actionDay` IN ('2024-01-01');

INSERT INTO `callsMetricsUnion` (
    `lostCallPackets`,
    `lostInternetPackets`,
    `timeSpentAtSite`,
    `dataBytesTotal`,
    `timeTotal`,
    `packetsTotal`,
    `client_id`,
    `client_phone_number`,
    `technology_name`,
    `cluster_name`,
    `zone_name`,
    `territory_name`,
    `actionDay`)

WITH `_generic_enrichedLog_calls_log` AS (
SELECT
    `calls_log`.`lost_packets_count` AS `lost_packets_count`,
    `calls_log`.`client_id` AS `client_id`,
    `calls_log`.`cell_id` AS `cell_id`,
    `calls_log`.`action_time` AS `action_time`,
    SUBSTRING(`calls_log`.`client_id` FROM 1 FOR 6) AS `operatorCode`,
    CASE
        WHEN `operatorCode` = '1234' THEN 'xtelecom'
        WHEN `operatorCode` = '1235' THEN 'ytelecom'
        WHEN NOT `operatorCode` LIKE '123%' THEN 'roaming'
        ELSE 'unknown' END AS `operatorName`,
    CASE
        WHEN CHAR_LENGTH(`client_phone_number`) = 10 AND NOT `client_phone_number` LIKE '1%' THEN '1' || `client_phone_number`
        ELSE `client_phone_number` END AS `client_phone_number`,
    FLOOR(`calls_log`.`action_time` TO 'day') AS `actionDay`
FROM `calls_log.csv` AS `calls_log`),

`_generic_logDimensions_calls_log` AS (
SELECT
    `l`.`lost_packets_count` AS `lost_packets_count`,
    `l`.`client_id` AS `client_id`,
    `l`.`cell_id` AS `cell_id`,
    `l`.`action_time` AS `action_time`,
    `l`.`operatorCode` AS `operatorCode`,
    `l`.`operatorName` AS `operatorName`,
    `l`.`client_phone_number` AS `client_phone_number`,
    `l`.`actionDay` AS `actionDay`,
    `c`.`technology_name` AS `technology_name`,
    `c`.`cluster_name` AS `cluster_name`,
    `c`.`zone_name` AS `zone_name`,
    `c`.`territory_name` AS `territory_name`
FROM `_generic_enrichedLog_calls_log` AS `l`
INNER JOIN `cells.csv` AS `c` ON `c`.`id` = `l`.`cell_id`)

(SELECT
    SUM(`cl`.`lost_packets_count`) AS `lostCallPackets`,
    NULL AS `lostInternetPackets`,
    NULL AS `timeSpentAtSite`,
    NULL AS `dataBytesTotal`,
    NULL AS `timeTotal`,
    NULL AS `packetsTotal`,
    `cl`.`client_id` AS `client_id`,
    `cl`.`client_phone_number` AS `client_phone_number`,
    `cl`.`technology_name` AS `technology_name`,
    `cl`.`cluster_name` AS `cluster_name`,
    `cl`.`zone_name` AS `zone_name`,
    `cl`.`territory_name` AS `territory_name`,
    `cl`.`actionDay` AS `actionDay`
FROM `_generic_logDimensions_calls_log` AS `cl`
WHERE `actionDay` IN ('2024-01-01')
GROUP BY `client_id`, `client_phone_number`, `technology_name`, `cluster_name`, `zone_name`, `territory_name`, `actionDay`);

DELETE FROM `cellConnectionMetricsUnion`
WHERE `actionDay` IN ('2024-01-01');

INSERT INTO `cellConnectionMetricsUnion` (
    `timeSpentAtSite`,
    `lostCallPackets`,
    `lostInternetPackets`,
    `dataBytesTotal`,
    `timeTotal`,
    `packetsTotal`,
    `client_id`,
    `client_phone_number`,
    `technology_name`,
    `cluster_name`,
    `zone_name`,
    `territory_name`,
    `actionDay`)

WITH `_generic_enrichedLog_cell_connection_log` AS (
SELECT
    `cell_connection_log`.`client_id` AS `client_id`,
    `cell_connection_log`.`cell_id` AS `cell_id`,
    `cell_connection_log`.`action_time` AS `action_time`,
    SUBSTRING(`cell_connection_log`.`client_id` FROM 1 FOR 6) AS `operatorCode`,
    CASE
        WHEN `operatorCode` = '1234' THEN 'xtelecom'
        WHEN `operatorCode` = '1235' THEN 'ytelecom'
        WHEN NOT `operatorCode` LIKE '123%' THEN 'roaming'
        ELSE 'unknown' END AS `operatorName`,
    CASE
        WHEN CHAR_LENGTH(`client_phone_number`) = 10 AND NOT `client_phone_number` LIKE '1%' THEN '1' || `client_phone_number`
        ELSE `client_phone_number` END AS `client_phone_number`,
    FLOOR(`cell_connection_log`.`action_time` TO 'day') AS `actionDay`
FROM `cell_connection_log.csv` AS `cell_connection_log`),

`_generic_logDimensions_cell_connection_log` AS (
SELECT
    `l`.`client_id` AS `client_id`,
    `l`.`cell_id` AS `cell_id`,
    `l`.`action_time` AS `action_time`,
    `l`.`operatorCode` AS `operatorCode`,
    `l`.`operatorName` AS `operatorName`,
    `l`.`client_phone_number` AS `client_phone_number`,
    `l`.`actionDay` AS `actionDay`,
    `c`.`technology_name` AS `technology_name`,
    `c`.`cluster_name` AS `cluster_name`,
    `c`.`zone_name` AS `zone_name`,
    `c`.`territory_name` AS `territory_name`
FROM `_generic_enrichedLog_cell_connection_log` AS `l`
INNER JOIN `cells.csv` AS `c` ON `c`.`id` = `l`.`cell_id`)

(SELECT
    CASE
        WHEN ROUND((
            MAX(COALESCE(
                LEAD(`ccl`.`action_time`, 1, NULL) OVER (PARTITION BY `ccl`.`client_id`, `ccl`.`client_phone_number`, `ccl`.`actionDay` ORDER BY `ccl`.`client_id`, `ccl`.`action_time`),
                `ccl`.`action_time`)
            ) - MIN(`ccl`.`action_time`)
         ) / 60) <= 0 THEN 5
        ELSE ROUND((
            MAX(COALESCE(
                LEAD(`ccl`.`action_time`, 1, NULL) OVER (PARTITION BY `ccl`.`client_id`, `ccl`.`client_phone_number`, `ccl`.`actionDay` ORDER BY `ccl`.`client_id`, `ccl`.`action_time`),
                `ccl`.`action_time`)
            ) - MIN(`ccl`.`action_time`)
        ) / 60) END AS `timeSpentAtSite`,
    NULL AS `lostCallPackets`,
    NULL AS `lostInternetPackets`,
    NULL AS `dataBytesTotal`,
    NULL AS `timeTotal`,
    NULL AS `packetsTotal`,
    `ccl`.`client_id` AS `client_id`,
    `ccl`.`client_phone_number` AS `client_phone_number`,
    `ccl`.`technology_name` AS `technology_name`,
    `ccl`.`cluster_name` AS `cluster_name`,
    `ccl`.`zone_name` AS `zone_name`,
    `ccl`.`territory_name` AS `territory_name`,
    `ccl`.`actionDay` AS `actionDay`
FROM `_generic_logDimensions_cell_connection_log` AS `ccl`
WHERE `actionDay` IN ('2024-01-01')
GROUP BY `client_id`, `client_phone_number`, `technology_name`, `cluster_name`, `zone_name`, `territory_name`, `actionDay`);


DELETE FROM `internetUsageMetricsUnion`
WHERE `actionDay` IN ('2024-01-01');

INSERT INTO `internetUsageMetricsUnion` (
    `dataBytesTotal`,
    `timeTotal`,
    `packetsTotal`,
    `lostInternetPackets`,
    `lostCallPackets`,
    `timeSpentAtSite`,
    `client_id`,
    `client_phone_number`,
    `technology_name`,
    `cluster_name`,
    `zone_name`,
    `territory_name`,
    `actionDay`)

WITH `_generic_enrichedLog_internet_usage_log` AS (

SELECT
    `internet_usage_log`.`upload_bytes_count` AS `upload_bytes_count`,
    `internet_usage_log`.`download_bytes_count` AS `download_bytes_count`,
    `internet_usage_log`.`upload_time_millis` AS `upload_time_millis`,
    `internet_usage_log`.`download_time_millis` AS `download_time_millis`,
    `internet_usage_log`.`upload_retransmitted_packets_count` AS `upload_retransmitted_packets_count`,
    `internet_usage_log`.`upload_packets_count` AS `upload_packets_count`,
    `internet_usage_log`.`download_retransmitted_packets_count` AS `download_retransmitted_packets_count`,
    `internet_usage_log`.`download_packets_count` AS `download_packets_count`,
    `internet_usage_log`.`client_id` AS `client_id`,
    `internet_usage_log`.`cell_id` AS `cell_id`,
    `internet_usage_log`.`action_time` AS `action_time`,
    SUBSTRING(`internet_usage_log`.`client_id` FROM 1 FOR 6) AS `operatorCode`,
    CASE
        WHEN `operatorCode` = '1234' THEN 'xtelecom'
        WHEN `operatorCode` = '1235' THEN 'ytelecom'
        WHEN NOT `operatorCode` LIKE '123%' THEN 'roaming'
        ELSE 'unknown' END AS `operatorName`,
    CASE
        WHEN CHAR_LENGTH(`client_phone_number`) = 10 AND NOT `client_phone_number` LIKE '1%' THEN '1' || `client_phone_number`
        ELSE `client_phone_number` END AS `client_phone_number`,
    FLOOR(`internet_usage_log`.`action_time` TO 'day') AS `actionDay`
FROM `internet_usage_log.csv` AS `internet_usage_log`),

`_generic_logDimensions_internet_usage_log` AS (
SELECT
    `l`.`upload_bytes_count` AS `upload_bytes_count`,
    `l`.`download_bytes_count` AS `download_bytes_count`,
    `l`.`upload_time_millis` AS `upload_time_millis`,
    `l`.`download_time_millis` AS `download_time_millis`,
    `l`.`upload_retransmitted_packets_count` AS `upload_retransmitted_packets_count`,
    `l`.`upload_packets_count` AS `upload_packets_count`,
    `l`.`download_retransmitted_packets_count` AS `download_retransmitted_packets_count`,
    `l`.`download_packets_count` AS `download_packets_count`,
    `l`.`client_id` AS `client_id`,
    `l`.`cell_id` AS `cell_id`,
    `l`.`action_time` AS `action_time`,
    `l`.`operatorCode` AS `operatorCode`,
    `l`.`operatorName` AS `operatorName`,
    `l`.`client_phone_number` AS `client_phone_number`,
    `l`.`actionDay` AS `actionDay`,
    `c`.`technology_name` AS `technology_name`,
    `c`.`cluster_name` AS `cluster_name`,
    `c`.`zone_name` AS `zone_name`,
    `c`.`territory_name` AS `territory_name`
FROM `_generic_enrichedLog_internet_usage_log` AS `l`
INNER JOIN `cells.csv` AS `c` ON `c`.`id` = `l`.`cell_id`)

(SELECT
    SUM(`il`.`upload_bytes_count` + `il`.`download_bytes_count`) AS `dataBytesTotal`,
    SUM(`il`.`upload_time_millis` + `il`.`download_time_millis`) AS `timeTotal`,
    SUM(`il`.`upload_packets_count` + `il`.`download_packets_count`) AS `packetsTotal`,
    SUM(`il`.`upload_retransmitted_packets_count` - (`il`.`upload_packets_count`
         + (`il`.`download_retransmitted_packets_count` - `il`.`download_packets_count`))) AS `lostInternetPackets`,
    NULL AS `lostCallPackets`,
    NULL AS `timeSpentAtSite`,
    `il`.`client_id` AS `client_id`,
    `il`.`client_phone_number` AS `client_phone_number`,
    `il`.`technology_name` AS `technology_name`,
    `il`.`cluster_name` AS `cluster_name`,
    `il`.`zone_name` AS `zone_name`,
    `il`.`territory_name` AS `territory_name`,
    `il`.`actionDay` AS `actionDay`
FROM `_generic_logDimensions_internet_usage_log` AS `il`
WHERE `actionDay` IN ('2024-01-01')
GROUP BY `client_id`, `client_phone_number`, `technology_name`, `cluster_name`, `zone_name`, `territory_name`, `actionDay`);


DELETE FROM `customerBehaviorMetrics`
WHERE `actionDay` IN ('2024-01-01');

INSERT INTO `customerBehaviorMetrics` (
    `dataBytesTotal`,
    `timeTotal`,
    `packetsTotal`,
    `lostInternetPackets`,
    `lostCallPackets`,
    `timeSpentAtSite`,
    `client_id`,
    `client_phone_number`,
    `technology_name`,
    `cluster_name`,
    `zone_name`,
    `territory_name`,
    `actionDay`)

WITH `customerBehaviorMetricsUnion` AS (

SELECT
    `t`.`dataBytesTotal` AS `dataBytesTotal`,
    `t`.`timeTotal` AS `timeTotal`,
    `t`.`packetsTotal` AS `packetsTotal`,
    `t`.`lostInternetPackets` AS `lostInternetPackets`,
    `t`.`lostCallPackets` AS `lostCallPackets`,
    `t`.`timeSpentAtSite` AS `timeSpentAtSite`,
    `t`.`client_id` AS `client_id`,
    `t`.`client_phone_number` AS `client_phone_number`,
    `t`.`technology_name` AS `technology_name`,
    `t`.`cluster_name` AS `cluster_name`,
    `t`.`zone_name` AS `zone_name`,
    `t`.`territory_name` AS `territory_name`,
    `t`.`actionDay` AS `actionDay`
FROM `internetUsageMetricsUnion` AS `t`
UNION
SELECT
    `t`.`dataBytesTotal` AS `dataBytesTotal`,
    `t`.`timeTotal` AS `timeTotal`,
    `t`.`packetsTotal` AS `packetsTotal`,
    `t`.`lostInternetPackets` AS `lostInternetPackets`,
    `t`.`lostCallPackets` AS `lostCallPackets`,
    `t`.`timeSpentAtSite` AS `timeSpentAtSite`,
    `t`.`client_id` AS `client_id`,
    `t`.`client_phone_number` AS `client_phone_number`,
    `t`.`technology_name` AS `technology_name`,
    `t`.`cluster_name` AS `cluster_name`,
    `t`.`zone_name` AS `zone_name`,
    `t`.`territory_name` AS `territory_name`,
    `t`.`actionDay` AS `actionDay`
FROM `callsMetricsUnion` AS `t`
UNION
SELECT
    `t`.`dataBytesTotal` AS `dataBytesTotal`,
    `t`.`timeTotal` AS `timeTotal`,
    `t`.`packetsTotal` AS `packetsTotal`,
    `t`.`lostInternetPackets` AS `lostInternetPackets`,
    `t`.`lostCallPackets` AS `lostCallPackets`,
    `t`.`timeSpentAtSite` AS `timeSpentAtSite`,
    `t`.`client_id` AS `client_id`,
    `t`.`client_phone_number` AS `client_phone_number`,
    `t`.`technology_name` AS `technology_name`,
    `t`.`cluster_name` AS `cluster_name`,
    `t`.`zone_name` AS `zone_name`,
    `t`.`territory_name` AS `territory_name`, `t`.`actionDay` AS `actionDay`
FROM `cellConnectionMetricsUnion` AS `t`)

(SELECT
    MAX(`p`.`dataBytesTotal`) AS `dataBytesTotal`,
    MAX(`p`.`timeTotal`) AS `timeTotal`,
    MAX(`p`.`packetsTotal`) AS `packetsTotal`,
    MAX(`p`.`lostInternetPackets`) AS `lostInternetPackets`,
    MAX(`p`.`lostCallPackets`) AS `lostCallPackets`,
    MAX(`p`.`timeSpentAtSite`) AS `timeSpentAtSite`,
    `p`.`client_id` AS `client_id`,
    `p`.`client_phone_number` AS `client_phone_number`,
    `p`.`technology_name` AS `technology_name`,
    `p`.`cluster_name` AS `cluster_name`,
    `p`.`zone_name` AS `zone_name`,
    `p`.`territory_name` AS `territory_name`,
    `p`.`actionDay` AS `actionDay`
FROM `customerBehaviorMetricsUnion` AS `p`
WHERE `actionDay` IN ('2024-01-01')
GROUP BY `client_id`, `client_phone_number`, `technology_name`, `cluster_name`, `zone_name`, `territory_name`, `actionDay`);