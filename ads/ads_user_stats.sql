DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `ds`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_stats/';


insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '2025-06-27' as ds,
       recent_days,
       sum(if(login_date_first >= date_add('2025-06-27', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where ds = '2025-06-27'
  and login_date_last >= date_add('2025-06-27', -recent_days + 1)
group by recent_days;