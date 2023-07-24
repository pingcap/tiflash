set tidb_allow_mpp=1; set tidb_enforce_mpp=1; set tidb_isolation_read_engines='tiflash';
select i_item_id,
    ca_country,
    ca_state,
    ca_county,
    avg( cast(cs_quantity as decimal(12,2))) agg1,
    avg( cast(cs_list_price as decimal(12,2))) agg2,
    avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
    avg( cast(cs_sales_price as decimal(12,2))) agg4,
    avg( cast(cs_net_profit as decimal(12,2))) agg5,
    avg( cast(c_birth_year as decimal(12,2))) agg6,
    avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
from test.catalog_sales, test.customer_demographics cd1,
    test.customer_demographics cd2, test.customer, test.customer_address, test.date_dim, test.item
where cs_sold_date_sk = d_date_sk and
    cs_item_sk = i_item_sk and
    cs_bill_cdemo_sk = cd1.cd_demo_sk and
    cs_bill_customer_sk = c_customer_sk and
    cd1.cd_gender = '1' and
    cd1.cd_education_status = '1' and
    c_current_cdemo_sk = cd2.cd_demo_sk and
    c_current_addr_sk = ca_address_sk and
    c_birth_month in (1,2,3,4,5,6) and
    d_year = 2000 and
    ca_state in ('1','2','3'
    ,'4','5','6','7')
group by i_item_id, ca_country, ca_state, ca_county with rollup
order by ca_country,
    ca_state,
    ca_county,
    i_item_id;