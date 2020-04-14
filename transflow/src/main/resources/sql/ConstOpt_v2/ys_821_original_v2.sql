-- 保理原受821表直连
select
    SN,
    INST_DATE,
    INST_TIME,
    SETTLE_DATE,
    MCHT_CD,
    MCHT_TYPE,
    TERM_ID,
    CARD_TYPE,
    CURRCY_CODE_TRANS,
    CARD_NO,
    ISSUE_BANK,
    TXN_TYPE,
    TXN_AMT,
    TRANS_AMT,
    FEE,
    TXN_STATUS,
    ORG_FLG,
    ORG_EXT,
    AREA_FLAG,
    ORI_DT,
    'bl_0'           as PRODUCT_NAME,
    0Y               as AGT_TYPE,
    current_date()   as create_time,
    current_user()   as create_user
from
(
    select
        row_number() over (partition by trim(sn)
            order by p_date desc )
                                                                        as row_n,
        trim(sn)                                                        as SN,
        to_date( from_unixtime(unix_timestamp(trim(tx_date),'yyyyMMdd'),'yyyy-MM-dd'))
                                                                        as INST_DATE,
        trim(tx_time)                                                   as INST_TIME,
        to_date( from_unixtime(unix_timestamp(trim(settle_time),'yyyyMMdd'),'yyyy-MM-dd'))
                                                                        as SETTLE_DATE,
        trim(mcht_cd)                                                   as MCHT_CD,
        trim(mcht_type)                                                 as MCHT_TYPE,
        trim(device_id)                                                 as TERM_ID,
        trim(card_type)                                                 as CARD_TYPE,
        trim(currency)                                                  as CURRCY_CODE_TRANS,
        trim(card_no)                                                   as CARD_NO,
        trim(card_bank)                                                 as ISSUE_BANK,
        trim(trans_type)                                                as TXN_TYPE,
        cast(cast(trim(trans_amt) as decimal(15,2))*100 as bigint )     as TXN_AMT,
        cast(cast(trim(settle_amt) as decimal(15,2))*100 as bigint )    as TRANS_AMT,
        cast(cast(trim(fee) as decimal(15,2))*100 as bigint )           as FEE,
        case when trim(trans_status)= '成功' then '处理成功' else trim(trans_status) end
                                                                        as TXN_STATUS,
        trim(card_inst)                                                 as ORG_FLG,
        substring(trim(card_inst),1,4)                                  as ORG_EXT,
        trim(trans_area)                                                as AREA_FLAG,
        trim(origin_time)                                               as ORI_DT
    from rds_posflow.loginfo_rsp_bl
    where
        p_date >= date_add( '${1}' , -2 ) and
        p_date <  date_add( '${2}' , 14 ) and
        from_unixtime(unix_timestamp(trim(tx_date), 'yyyyMMdd'), 'yyyy-MM-dd') >= '${1}' and
        from_unixtime(unix_timestamp(trim(tx_date), 'yyyyMMdd'), 'yyyy-MM-dd') <  '${2}'
) ttt
where row_n = 1