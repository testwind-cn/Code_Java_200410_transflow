INSERT OVERWRITE TABLE rds_posflow.bl_flow_by_week_aif
    PARTITION (p_week, inst_date)
select
    mcht_cd
    ,SUM(flow_amt_sell)             AS flow_amt_sell
    ,SUM(flow_vol_sell)             AS flow_vol_sell
    ,SUM(flow_amt_return)           AS flow_amt_return
    ,SUM(flow_vol_return)           AS flow_vol_return
    ,SUM(CASE WHEN flow_amt_sell > 0 THEN 1 ELSE 0 END)
                                    AS active_day
    ,datediff(date_sub(current_date(), 0-1),'2000-01-02') % 7
                                    AS p_week
    ,p_inst_date                    AS inst_date
from
(
    select
        mcht_cd

        ,flow_amt_sell
        ,flow_vol_sell
        ,flow_amt_return
        ,flow_vol_return

        ,inst_date,

        CASE -- 不用减日期，正好搭在等于日期上，这里多减3天，是防止漏跑批的情况下，3天内都可以工作
            WHEN inst_date >= date_sub(current_date(), 7-1) AND inst_date < date_sub(current_date(), 0-1) THEN date_sub(current_date(), 7-1)
            WHEN inst_date >= date_sub(current_date(),14-1) AND inst_date < date_sub(current_date(), 7-1) THEN date_sub(current_date(),14-1)
            WHEN inst_date >= date_sub(current_date(),21-1) AND inst_date < date_sub(current_date(),14-1) THEN date_sub(current_date(),21-1)
            WHEN inst_date >= date_sub(current_date(),28-1) AND inst_date < date_sub(current_date(),21-1) THEN date_sub(current_date(),28-1)
            WHEN inst_date >= date_sub(current_date(),35-1) AND inst_date < date_sub(current_date(),28-1) THEN date_sub(current_date(),35-1)
            WHEN inst_date >= date_sub(current_date(),42-1) AND inst_date < date_sub(current_date(),35-1) THEN date_sub(current_date(),42-1)
            WHEN inst_date >= date_sub(current_date(),49-1) AND inst_date < date_sub(current_date(),42-1) THEN date_sub(current_date(),49-1)
            WHEN inst_date >= date_sub(current_date(),56-1) AND inst_date < date_sub(current_date(),49-1) THEN date_sub(current_date(),56-1)
            WHEN inst_date >= date_sub(current_date(),63-1) AND inst_date < date_sub(current_date(),56-1) THEN date_sub(current_date(),63-1)
            WHEN inst_date >= date_sub(current_date(),70-1) AND inst_date < date_sub(current_date(),63-1) THEN date_sub(current_date(),70-1)
            WHEN inst_date >= date_sub(current_date(),77-1) AND inst_date < date_sub(current_date(),70-1) THEN date_sub(current_date(),77-1)
            WHEN inst_date >= date_sub(current_date(),84-1) AND inst_date < date_sub(current_date(),77-1) THEN date_sub(current_date(),84-1)
        END AS p_inst_date
    from rds_posflow.bl_flow_by_day
    where
        inst_date >= date_sub(current_date(),84-1) AND inst_date < date_sub(current_date(), 0-1)
) t1
group by
    mcht_cd,p_inst_date