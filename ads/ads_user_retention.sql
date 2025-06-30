DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `ds`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_retention/';


insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '2025-06-27' ds,
       login_date_first create_date,
       datediff('2025-06-27', login_date_first) retention_day,
       sum(if(login_date_last = '2025-06-27', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '2025-06-27', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where ds = '2025-06-27'
           and login_date_first >= date_add('2025-06-27', -7)
           and login_date_first < '2025-06-27'
     ) t1
group by login_date_first;