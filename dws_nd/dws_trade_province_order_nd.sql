DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dws/dws_trade_province_order_nd';

insert overwrite table dws_trade_province_order_nd partition(ds='2025-06-27')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(ds>=date_add('2025-06-27',-6),order_count_1d,0)),
    sum(if(ds>=date_add('2025-06-27',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-27',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-27',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-06-27',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where ds>=date_add('2025-06-27',-29)
and ds<='2025-06-27'
group by province_id,province_name,area_code,iso_code,iso_3166_2;