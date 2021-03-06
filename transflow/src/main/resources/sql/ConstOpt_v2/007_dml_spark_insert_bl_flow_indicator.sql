SET hivevar:MAIN_DB=rds_posflow;
SET hivevar:TEMP_DB=deprecated_db;

INSERT OVERWRITE TABLE ${hivevar:MAIN_DB}.bl_flow_indicator
    PARTITION (inst_date)
SELECT
    t_month.mcht_cd
    ,t_month.agt_type
    ,t_month.product_name1
    ,t_month.product_name2

    ,t_month.amt_1M
    ,t_month.amt_2M
    ,t_month.amt_3M
    ,t_month.amt_6M
    ,t_month.amt_12M
    ,t_month.amt_1M_mir
    ,t_month.amt_3M_mir
    ,t_month.amt_6M_mir
    ,t_month.amt_12M_1
    ,t_month.amt_12M_2
    ,t_month.amt_12M_3
    ,t_month.amt_12M_4
    ,t_month.amt_12M_5
    ,t_month.amt_12M_6
    ,t_month.amt_12M_7
    ,t_month.amt_12M_8
    ,t_month.amt_12M_9
    ,t_month.amt_12M_10
    ,t_month.amt_12M_11
    ,t_month.amt_12M_12
    ,t_month.active_days_1M

    ,coalesce(t_week.amt_9W_1,0) as amt_9W_1
    ,coalesce(t_week.amt_9W_2,0) as amt_9W_2
    ,coalesce(t_week.amt_9W_3,0) as amt_9W_3
    ,coalesce(t_week.amt_9W_4,0) as amt_9W_4
    ,coalesce(t_week.amt_9W_5,0) as amt_9W_5
    ,coalesce(t_week.amt_9W_6,0) as amt_9W_6
    ,coalesce(t_week.amt_9W_7,0) as amt_9W_7
    ,coalesce(t_week.amt_9W_8,0) as amt_9W_8
    ,coalesce(t_week.amt_9W_9,0) as amt_9W_9

    ,'${1}' as inst_date
FROM
(
    SELECT
        mcht_cd
        ,MAX(agt_type)                  AS agt_type
        ,MAX(product_name1)             AS product_name1
        ,MIN(product_name2)             AS product_name2

        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 30) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_1M
        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 60) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_2M
        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 90) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_3M
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',180) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_6M
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',360) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_12M

        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 60) AND inst_date < date_sub('${1}', 30) THEN flow_amt_sell ELSE 0 END) AS amt_1M_mir
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',180) AND inst_date < date_sub('${1}', 90) THEN flow_amt_sell ELSE 0 END) AS amt_3M_mir
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',360) AND inst_date < date_sub('${1}',180) THEN flow_amt_sell ELSE 0 END) AS amt_6M_mir

        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 30) AND inst_date < date_sub('${1}',  0) THEN flow_amt_sell ELSE 0 END) AS amt_12M_1
        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 60) AND inst_date < date_sub('${1}', 30) THEN flow_amt_sell ELSE 0 END) AS amt_12M_2
        ,SUM(CASE WHEN inst_date >= date_sub('${1}', 90) AND inst_date < date_sub('${1}', 60) THEN flow_amt_sell ELSE 0 END) AS amt_12M_3
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',120) AND inst_date < date_sub('${1}', 90) THEN flow_amt_sell ELSE 0 END) AS amt_12M_4
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',150) AND inst_date < date_sub('${1}',120) THEN flow_amt_sell ELSE 0 END) AS amt_12M_5
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',180) AND inst_date < date_sub('${1}',150) THEN flow_amt_sell ELSE 0 END) AS amt_12M_6
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',210) AND inst_date < date_sub('${1}',180) THEN flow_amt_sell ELSE 0 END) AS amt_12M_7
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',240) AND inst_date < date_sub('${1}',210) THEN flow_amt_sell ELSE 0 END) AS amt_12M_8
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',270) AND inst_date < date_sub('${1}',240) THEN flow_amt_sell ELSE 0 END) AS amt_12M_9
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',300) AND inst_date < date_sub('${1}',270) THEN flow_amt_sell ELSE 0 END) AS amt_12M_10
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',330) AND inst_date < date_sub('${1}',300) THEN flow_amt_sell ELSE 0 END) AS amt_12M_11
        ,SUM(CASE WHEN inst_date >= date_sub('${1}',360) AND inst_date < date_sub('${1}',330) THEN flow_amt_sell ELSE 0 END) AS amt_12M_12

        ,MAX(CASE WHEN inst_date >= date_sub('${1}', 30) AND inst_date < date_sub('${1}',  0) THEN active_day    ELSE 0 END) AS active_days_1M

    FROM
        ${hivevar:MAIN_DB}.bl_flow_by_30day
    where
        p_day = datediff('${1}','2000-01-02') % 30 AND
        inst_date <  date_sub('${1}', 0) AND
        inst_date >= date_sub('${1}', 360)
    group by
        mcht_cd
) t_month
LEFT JOIN
(
    SELECT
        mcht_cd,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),   7) AND inst_date < date_sub(date_add('${1}',1), 0) THEN flow_amt_sell ELSE 0 END) AS amt_9W_1,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  14) AND inst_date < date_sub(date_add('${1}',1), 7) THEN flow_amt_sell ELSE 0 END) AS amt_9W_2,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  21) AND inst_date < date_sub(date_add('${1}',1),14) THEN flow_amt_sell ELSE 0 END) AS amt_9W_3,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  28) AND inst_date < date_sub(date_add('${1}',1),21) THEN flow_amt_sell ELSE 0 END) AS amt_9W_4,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  35) AND inst_date < date_sub(date_add('${1}',1),28) THEN flow_amt_sell ELSE 0 END) AS amt_9W_5,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  42) AND inst_date < date_sub(date_add('${1}',1),35) THEN flow_amt_sell ELSE 0 END) AS amt_9W_6,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  49) AND inst_date < date_sub(date_add('${1}',1),42) THEN flow_amt_sell ELSE 0 END) AS amt_9W_7,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  56) AND inst_date < date_sub(date_add('${1}',1),49) THEN flow_amt_sell ELSE 0 END) AS amt_9W_8,
        SUM(CASE WHEN inst_date >= date_sub(date_add('${1}',1),  63) AND inst_date < date_sub(date_add('${1}',1),56) THEN flow_amt_sell ELSE 0 END) AS amt_9W_9
    FROM
        ${hivevar:MAIN_DB}.bl_flow_by_week
    where
        p_week = datediff(date_add('${1}',1),'2000-01-02') % 7 AND
        inst_date <  date_sub(date_add('${1}',1), 0) AND
        inst_date >= date_sub(date_add('${1}',1), 63)
    group by
        mcht_cd
) t_week
ON  t_month.mcht_cd = t_week.mcht_cd
