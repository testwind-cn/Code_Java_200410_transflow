INSERT OVERWRITE TABLE deprecated_db.bl_flow_by_week_01
    PARTITION (p_week, inst_date)
select
    mcht_cd
    ,SUM(flow_amt_sell)             AS flow_amt_sell
    ,SUM(flow_vol_sell)             AS flow_vol_sell
    ,SUM(flow_amt_return)           AS flow_amt_return
    ,SUM(flow_vol_return)           AS flow_vol_return
    ,SUM(CASE WHEN flow_amt_sell > 0 THEN 1 ELSE 0 END)
                                    AS active_day
    ,datediff(current_date(),'2000-01-02') % 7
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
            WHEN inst_date >= date_sub(current_date(), 7) AND inst_date < date_sub(current_date(), 0) THEN date_sub(current_date(), 7)
            WHEN inst_date >= date_sub(current_date(),14) AND inst_date < date_sub(current_date(), 7) THEN date_sub(current_date(),14)
            WHEN inst_date >= date_sub(current_date(),21) AND inst_date < date_sub(current_date(),14) THEN date_sub(current_date(),21)
            WHEN inst_date >= date_sub(current_date(),28) AND inst_date < date_sub(current_date(),21) THEN date_sub(current_date(),28)
            WHEN inst_date >= date_sub(current_date(),35) AND inst_date < date_sub(current_date(),28) THEN date_sub(current_date(),35)
            WHEN inst_date >= date_sub(current_date(),42) AND inst_date < date_sub(current_date(),35) THEN date_sub(current_date(),42)
            WHEN inst_date >= date_sub(current_date(),49) AND inst_date < date_sub(current_date(),42) THEN date_sub(current_date(),49)
            WHEN inst_date >= date_sub(current_date(),56) AND inst_date < date_sub(current_date(),49) THEN date_sub(current_date(),56)
            WHEN inst_date >= date_sub(current_date(),63) AND inst_date < date_sub(current_date(),56) THEN date_sub(current_date(),63)
            WHEN inst_date >= date_sub(current_date(),70) AND inst_date < date_sub(current_date(),63) THEN date_sub(current_date(),70)
            WHEN inst_date >= date_sub(current_date(),77) AND inst_date < date_sub(current_date(),70) THEN date_sub(current_date(),77)
            WHEN inst_date >= date_sub(current_date(),84) AND inst_date < date_sub(current_date(),77) THEN date_sub(current_date(),84)
        END AS p_inst_date
    from rds_posflow.bl_flow_by_day
    where
        inst_date >= date_sub(current_date(),84) AND inst_date < date_sub(current_date(), 0)
) t1
group by
    mcht_cd,p_inst_date