DROP TABLE IF EXISTS ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE ads_order_to_pay_interval_avg
(
    `ds`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_to_pay_interval_avg/';


insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '2025-06-27',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd_trade_trade_flow_acc
where ds in ('9999-12-31','2025-06-27')
and payment_date_id='2025-06-27';