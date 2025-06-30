DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `ds`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_change/';

insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.ds,
    user_churn_count,
    user_back_count
from
(
    select
        '2025-06-27' ds,
        count(*) user_churn_count
    from dws_user_user_login_td
    where ds='2025-06-27'
    and login_date_last=date_add('2025-06-27',-7)
)churn
join
(
    select
        '2025-06-27' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where ds='2025-06-27'
        and login_date_last = '2025-06-27'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where ds=date_add('2025-06-27',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.ds=back.dt;