DROP TABLE IF EXISTS ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads_order_continuously_user_count
(
    `ds`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_continuously_user_count/';


insert overwrite table ads_order_continuously_user_count
select * from ads_order_continuously_user_count
union
select
    '2025-06-27',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(ds,2,'9999-12-31') over(partition by user_id order by ds),ds) diff
    from dws_trade_user_order_1d
    where ds>=date_add('2025-06-27',-6)
)t1
where diff=2;