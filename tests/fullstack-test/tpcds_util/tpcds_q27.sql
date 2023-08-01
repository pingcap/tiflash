set tidb_allow_mpp=1; set tidb_enforce_mpp=1; set tidb_isolation_read_engines='tiflash';
select i_item_id,
    s_state, grouping(s_state) g_state,
    avg(ss_quantity) agg1,
    avg(ss_list_price) agg2,
    avg(ss_coupon_amt) agg3,
    avg(ss_sales_price) agg4
from test.store_sales, test.customer_demographics, test.date_dim, test.store, test.item
where ss_sold_date_sk = d_date_sk and
    ss_item_sk = i_item_sk and
    ss_store_sk = s_store_sk and
    ss_cdemo_sk = cd_demo_sk and
    cd_gender = '1' and
    cd_marital_status = '1' and
    cd_education_status = '1' and
    d_year = 2000 and
    s_state in ('1','2', '3', '4', '5', '6')
group by i_item_id, s_state with rollup
order by i_item_id
        ,s_state;