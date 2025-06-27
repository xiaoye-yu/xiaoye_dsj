DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS parquet
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_cart_full/';

insert overwrite table dwd_trade_cart_full partition(ds='2025-06-27')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='20250627'
and is_ordered='0' limit 300;

insert into table dwd_trade_cart_full partition(ds='2025-06-25')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where is_ordered='0' limit 200;