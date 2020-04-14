-- 插入到hive
INSERT OVERWRITE TABLE rds_posflow.bl_total_transflow_v2_01
    PARTITION (inst_date)
SELECT
    sn,
    inst_time,
    settle_date,
    mcht_cd,
    mcht_type,
    term_id,
    card_type,
    currcy_code_trans,
    card_no,
    issue_bank,
    txn_type,
    txn_amt,
    trans_amt,
    fee,
    txn_status,
    org_flg,
    area_flag,
    ori_dt,
    product_name,
    agt_type,
    create_time,
    create_user,
    inst_date
from tmp_pos_flow_all