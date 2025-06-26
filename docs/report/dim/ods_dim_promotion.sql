DROP TABLE IF EXISTS ods_promotion_pos;
CREATE EXTERNAL TABLE ods_promotion_pos
(
    `id`                   STRING COMMENT '营销坑位ID',
    `pos_location`       STRING COMMENT '营销坑位位置',
    `pos_type`            STRING COMMENT '营销坑位类型：banner,宫格,列表,瀑布',
    `promotion_type`     STRING COMMENT '营销类型：算法、固定、搜索',
    `create_time`         STRING COMMENT '创建时间',
    `operate_time`        STRING COMMENT '修改时间'
) COMMENT '营销坑位表'
    PARTITIONED BY (`ds` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/bigdata_warehouse/gmall/ods/ods_promotion_pos_full/';

DROP TABLE IF EXISTS dim_promotion_pos_full;
CREATE EXTERNAL TABLE dim_promotion_pos_full
(
    `id`                 STRING COMMENT '营销坑位ID',
    `pos_location`     STRING COMMENT '营销坑位位置',
    `pos_type`          STRING COMMENT '营销坑位类型 ',
    `promotion_type`   STRING COMMENT '营销类型',
    `create_time`       STRING COMMENT '创建时间',
    `operate_time`      STRING COMMENT '修改时间'
) COMMENT '营销坑位维度表'
    PARTITIONED BY (`ds` STRING)
    STORED AS parquet
    LOCATION '/bigdata_warehouse/gmall/dim/dim_promotion_pos_full/';

insert overwrite table dim_promotion_pos_full partition(ds='20250625')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos
where ds='20250625';