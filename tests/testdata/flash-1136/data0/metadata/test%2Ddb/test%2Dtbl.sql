ATTACH TABLE `test-tbl`
(
    pk Nullable(Int32),
    _tidb_rowid Int64
)
ENGINE = DeltaMerge(_tidb_rowid, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"pk","O":"pk"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":66,"name":{"L":"test-tbl","O":"test-tbl"},"partition":null,"pk_is_handle":false,"schema_version":36,"state":5,"update_timestamp":417293448885567496}')
