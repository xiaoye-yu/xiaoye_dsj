DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
    PARTITIONED BY (`ds` STRING)
    STORED AS parquet
    LOCATION '/bigdata_warehouse/gmall/dim/dim_user_zip/';


insert overwrite table dim_user_zip partition (ds = '9999-12-31')
select data.id,
       concat(substr(data.name, 1, 1), '*')                name,
       if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
       if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(data.email, '@')[1]), null)   email,
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '20250624'                                        start_date,
       '9999-12-31'                                        end_date
from ods_user_info data
where ds = '20250625';

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dim_user_zip PARTITION (ds)
SELECT
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date,
    CASE
        WHEN rn = 1 THEN '9999-12-31'
        ELSE '20250624'
    END AS ds
FROM (
    SELECT
        id,
        -- 脱敏处理
        CASE WHEN name IS NOT NULL THEN CONCAT(SUBSTRING(name, 1, 1), '*') ELSE NULL END AS name,
        CASE
            WHEN phone_num RLIKE '^1[3-9][0-9]{9}$' THEN CONCAT(SUBSTRING(phone_num, 1, 3), '****', SUBSTRING(phone_num, 8))
            ELSE NULL
        END AS phone_num,
        CASE
            WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$' THEN CONCAT('***@', SPLIT(email, '@')[1])
            ELSE NULL
        END AS email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        '20250624' AS start_date,
        CASE
            WHEN rn = 1 THEN '9999-12-31'
            ELSE '20250625'
        END AS end_date,
        rn
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_date DESC) AS rn
        FROM (
            -- 合并变更数据 + 历史快照
            SELECT
          CAST(id AS STRING) AS id,
  CAST(name AS STRING) AS name,
  CAST(phone_num AS STRING) AS phone_num,
  CAST(email AS STRING) AS email,
  CAST(user_level AS STRING) AS user_level,
  CAST(birthday AS STRING) AS birthday,  -- ⭐ 这里关键
  CAST(gender AS STRING) AS gender,
  CAST(create_time AS STRING) AS create_time,
  CAST(operate_time AS STRING) AS operate_time,
  '2025062' AS start_date,
  '9999-12-31' AS end_date
            FROM ods_user_info
            WHERE ds = '20250625'

            UNION ALL

            SELECT
                id,
                name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                start_date,
                end_date
            FROM dim_user_zip
            WHERE ds = '9999-12-31'
        ) merged_data
    ) deduped_data
) final_data;

