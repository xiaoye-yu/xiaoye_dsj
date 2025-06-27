DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
CREATE EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
(
    `user_id`          STRING COMMENT '用户ID',
    `coupon_id`        STRING COMMENT '优惠券ID',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `benefit_rule`     STRING COMMENT '优惠规则',
    `used_count_1d`    STRING COMMENT '使用(支付)次数'
) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(ds)
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count,
    ds
from
(
    select
        ds,
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    group by ds,user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where ds='20250627'
)t2
on t1.coupon_id=t2.id;


insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(ds='2025-06-27')
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count
from
(
    select
        user_id,
        coupon_id,
        count(*) used_count
    from dwd_tool_coupon_used_inc
    where ds='2025-06-27'
    group by user_id,coupon_id
)t1
left join
(
    select
        id,
        coupon_name,
        coupon_type_code,
        coupon_type_name,
        benefit_rule
    from dim_coupon_full
    where ds='20250627'
)t2
on t1.coupon_id=t2.id;