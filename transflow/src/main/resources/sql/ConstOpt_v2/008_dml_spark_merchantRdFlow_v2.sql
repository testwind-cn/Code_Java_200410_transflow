INSERT OVERWRITE TABLE dm_unify.merchant_rd_flow_v2
    PARTITION (create_time)
SELECT
    mcht_cd         AS merchant_ap,
    MAX(agt_type)   AS agt_type,
    cast(SUM(amt_1M)/100 as decimal(13,2))     AS pos_amttxn_l1m,
    cast(SUM(amt_2M)/100 as decimal(13,2))     AS pos_amttxn_l2m,
    cast(SUM(amt_3M)/100 as decimal(13,2))     AS pos_amttxn_l3m,
    cast(SUM(amt_6M)/100 as decimal(13,2))     AS pos_amttxn_l6m,
    cast(SUM(amt_12M)/100 as decimal(13,2))    AS pos_amttxn_l12m,

    sum(
        if(amt_12M_1>0,1,0)+
        if(amt_12M_2>0,1,0)+
        if(amt_12M_3>0,1,0)
    )               AS pos_monthtxn_l3m,

    sum(
        if(amt_12M_1>0,1,0)+
        if(amt_12M_2>0,1,0)+
        if(amt_12M_3>0,1,0)+
        if(amt_12M_4>0,1,0)+
        if(amt_12M_5>0,1,0)+
        if(amt_12M_6>0,1,0)
    )               AS pos_monthtxn_l6m,

    sum(
        if(amt_12M_1 >0,1,0)+
        if(amt_12M_2 >0,1,0)+
        if(amt_12M_3 >0,1,0)+
        if(amt_12M_4 >0,1,0)+
        if(amt_12M_5 >0,1,0)+
        if(amt_12M_6 >0,1,0)+
        if(amt_12M_7 >0,1,0)+
        if(amt_12M_8 >0,1,0)+
        if(amt_12M_9 >0,1,0)+
        if(amt_12M_10>0,1,0)+
        if(amt_12M_11>0,1,0)+
        if(amt_12M_12>0,1,0)
    )               AS pos_monthtxn_l12m,

    sum(
        if(amt_8W_1>0,1,0)+
        if(amt_8W_2>0,1,0)+
        if(amt_8W_3>0,1,0)+
        if(amt_8W_4>0,1,0)
    )               AS pos_weektxn_l4w,

    sum(
        if(amt_8W_1>0,1,0)+
        if(amt_8W_2>0,1,0)+
        if(amt_8W_3>0,1,0)+
        if(amt_8W_4>0,1,0)+
        if(amt_8W_5>0,1,0)+
        if(amt_8W_6>0,1,0)+
        if(amt_8W_7>0,1,0)+
        if(amt_8W_8>0,1,0)
    )               AS pos_weektxn_l8w,

    max(active_days_1M) AS pos_daytxn_l30d,

    max(
        case
            when amt_12M_1 =0 then  1
            when amt_12M_2 =0 then  2
            when amt_12M_3 =0 then  3
            when amt_12M_4 =0 then  4
            when amt_12M_5 =0 then  5
            when amt_12M_6 =0 then  6
            when amt_12M_7 =0 then  7
            when amt_12M_8 =0 then  8
            when amt_12M_9 =0 then  9
            when amt_12M_10=0 then 10
            when amt_12M_11=0 then 11
            when amt_12M_12=0 then 12
            else                    0
        end
    )               AS pos_mon_sin_nop_l12m,

    sum(
        if(amt_12M_1-amt_12M_2>0,1,0)+
        if(amt_12M_2-amt_12M_3>0,1,0)+
        if(amt_12M_3-amt_12M_4>0,1,0)+
        if(amt_12M_4-amt_12M_5>0,1,0)+
        if(amt_12M_5-amt_12M_6>0,1,0)+
        if(amt_12M_6-amt_12M_7>0,1,0)
    )               AS pos_cnti_incr_l6m,

    sum(
        if(amt_12M_1 - amt_12M_2 >0,1,0)+
        if(amt_12M_2 - amt_12M_3 >0,1,0)+
        if(amt_12M_3 - amt_12M_4 >0,1,0)+
        if(amt_12M_4 - amt_12M_5 >0,1,0)+
        if(amt_12M_5 - amt_12M_6 >0,1,0)+
        if(amt_12M_6 - amt_12M_7 >0,1,0)+
        if(amt_12M_7 - amt_12M_8 >0,1,0)+
        if(amt_12M_8 - amt_12M_9 >0,1,0)+
        if(amt_12M_9 - amt_12M_10>0,1,0)+
        if(amt_12M_10- amt_12M_11>0,1,0)+
        if(amt_12M_11- amt_12M_12>0,1,0)
    )               AS pos_cnti_incr_l12m,

    sum(
        if(amt_12M_1 - amt_12M_2<=0,1,0)+
        if(amt_12M_2 - amt_12M_3<=0,1,0)+
        if(amt_12M_3 - amt_12M_4<=0,1,0)+
        if(amt_12M_4 - amt_12M_5<=0,1,0)+
        if(amt_12M_5 - amt_12M_6<=0,1,0)+
        if(amt_12M_6 - amt_12M_7<=0,1,0)
    )               AS pos_cnti_decr_l6m,

    sum(
        if(amt_12M_1 - amt_12M_2 <=0,1,0)+
        if(amt_12M_2 - amt_12M_3 <=0,1,0)+
        if(amt_12M_3 - amt_12M_4 <=0,1,0)+
        if(amt_12M_4 - amt_12M_5 <=0,1,0)+
        if(amt_12M_5 - amt_12M_6 <=0,1,0)+
        if(amt_12M_6 - amt_12M_7 <=0,1,0)+
        if(amt_12M_7 - amt_12M_8 <=0,1,0)+
        if(amt_12M_8 - amt_12M_9 <=0,1,0)+
        if(amt_12M_9 - amt_12M_10<=0,1,0)+
        if(amt_12M_10- amt_12M_11<=0,1,0)+
        if(amt_12M_11- amt_12M_12<=0,1,0)
    )               AS pos_cnti_decr_l12m,

    cast(max(round((amt_1M/amt_1M_mir-1)*100,2)) as decimal(13,2))      AS pos_incr_rte_l2m,
    cast(max(round((amt_3M/amt_3M_mir-1)*100,2)) as decimal(13,2))      AS pos_incr_rte_l6m,
    cast(max(round((amt_6M/amt_6M_mir-1)*100,2)) as decimal(13,2))      AS pos_incr_rte_l12m,
    cast(max(round(amt_3M/3,2))/100              as decimal(13,2))      AS pos_ave_pur_l3m,
    cast(max(round(amt_6M/6,2))/100              as decimal(13,2))      AS pos_ave_pur_l6m,
    cast(max(round(amt_12M/12,2))/100            as decimal(13,2))      AS pos_ave_pur_l12m,
    '0'                                     AS is_delete,
    current_user()                          AS create_user,
    current_date()                          AS modify_time,
    current_user()                          AS modify_user,
    current_date()                          AS create_time
FROM
(
    select
        mcht_cd
        ,agt_type

        ,amt_1M
        ,amt_2M
        ,amt_3M
        ,amt_6M
        ,amt_12M
        ,amt_1M_mir
        ,amt_3M_mir
        ,amt_6M_mir
        ,amt_12M_1
        ,amt_12M_2
        ,amt_12M_3
        ,amt_12M_4
        ,amt_12M_5
        ,amt_12M_6
        ,amt_12M_7
        ,amt_12M_8
        ,amt_12M_9
        ,amt_12M_10
        ,amt_12M_11
        ,amt_12M_12

        ,active_days_1M

        ,amt_9W_1   AS amt_8W_1
        ,amt_9W_2   AS amt_8W_2
        ,amt_9W_3   AS amt_8W_3
        ,amt_9W_4   AS amt_8W_4
        ,amt_9W_5   AS amt_8W_5
        ,amt_9W_6   AS amt_8W_6
        ,amt_9W_7   AS amt_8W_7
        ,amt_9W_8   AS amt_8W_8
        ,amt_9W_9   AS amt_8W_9
    from
        rds_posflow.bl_flow_indicator
    where
        inst_date = '${1}'

) b
GROUP BY mcht_cd
