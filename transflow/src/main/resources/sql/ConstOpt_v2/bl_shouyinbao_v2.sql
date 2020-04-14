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
    PRODUCT_NAME,
    2Y               as AGT_TYPE,
    current_date()   as create_time,
    current_user()   as create_user
from
(
    select
        row_number() over (partition by trim(pos.sn)
            order by abs(cast( trim(pos.trans_amt) as decimal(15,2) )) desc )
                                                                                    as row_n,
        trim(pos.sn)                                                                as SN
        , to_date( substring(trim(pos.trans_date), 1, 10) )                         as INST_DATE
        , translate(substring(trim(pos.trans_date), 12, 8), ':', '')                as INST_TIME
        , to_date( from_unixtime(unix_timestamp(trim(pos.inst_date),'yyyyMMdd'),'yyyy-MM-dd'))
                                                                                    as SETTLE_DATE
        , trim(pos.mcht_cd)                                                         as MCHT_CD
        , substring(trim(pos.mcht_cd), 8, 4)                                        as MCHT_TYPE
        , trim(pos.term_id)                                                         as TERM_ID
        , case when pan.sl_cd is null then '' else pan.sl_cd end                    as CARD_TYPE
        , '156'                                                                     as CURRCY_CODE_TRANS
        , trim(pos.trans_card)                                                      as CARD_NO
        , coalesce(cardbank1.card_org_name,cardbank2.card_org_name)                 as ISSUE_BANK
        , trxnum.sl_cd                                                              as TXN_TYPE
        , cast(cast(trim(pos.trans_amt) as decimal(15,2))*100 as bigint )           as TXN_AMT
        , cast((cast(trim(pos.trans_amt) as decimal(15,2))-cast(trim(pos.fee) as decimal(15,2)))*100 as bigint )
                                                                                    as TRANS_AMT
        , cast(cast(trim(pos.fee) as decimal(15,2))*100 as bigint )                 as FEE
        , trim(pos.trans_status)                                                    as TXN_STATUS
        , trim(pos.card_org)                                                        as ORG_FLG
        , CASE
            WHEN length(coalesce(trxnum.card_org,'')) > 0 THEN trxnum.card_org
            ELSE substring(trim(pos.card_org),1,4) END                              as ORG_EXT
        , trim(pos.p_branch)                                                        as AREA_FLAG
        , translate(concat(translate(substring(trim(pos.trans_date), 3, 8), '-', ''), translate(substring(trim(pos.trans_date), 12, 8), ':', '')), ' ', '')
                                                                                    as ORI_DT
        , trim(pos.product_name)                                                    as PRODUCT_NAME
    from rds_posflow.t1_trxrecprd_v2 pos
    left join rds_posflow.t99_trxnum_code_2020 trxnum
        on trim(pos.trans_type) = trxnum.syb_cd
    left join rds_posflow.t99_pan1_code pan
        on trim(pos.card_type) = pan.syb_cd
    left join rds_posflow.T99_cardbank_code cardbank1
        on trim(pos.card_org) = cardbank1.card_org_ref
    left join rds_posflow.T99_cardbank_code cardbank2
        on substring(trim(pos.card_org),1,4) = cardbank2.card_org_ref
    where
        p_date >= date_add( '${1}' , -2 ) and
        p_date <  date_add( '${2}' , 14 ) and
        to_date( substring(trim(pos.trans_date), 1, 10) ) >=  '${1}' and
        to_date( substring(trim(pos.trans_date), 1, 10) ) <   '${2}' and
        -- ( trxnum.sl_cd is null or
        ( trxnum.sl_cd is not null and trxnum.sl_cd <> '9999' )
) ttt
where row_n = 1