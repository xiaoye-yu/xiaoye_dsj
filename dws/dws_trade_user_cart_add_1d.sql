DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dws/dws_trade_user_cart_add_1d';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_cart_add_1d partition(ds)
select
    user_id,
    count(*),
    sum(sku_num),
    ds
from dwd_trade_cart_add_inc
group by user_id,ds;

insert overwrite table dws_trade_user_cart_add_1d partition(ds='2025-06-27')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd_trade_cart_add_inc
where ds='2025-06-27'
group by user_id;