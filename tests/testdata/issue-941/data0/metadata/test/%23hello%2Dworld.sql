ATTACH TABLE `#hello-world`
(
    t Nullable(Int32),
    _tidb_rowid Int64
)
ENGINE = DeltaMerge(_tidb_rowid, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"t","O":"t"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":45,"name":{"L":"#hello-world","O":"#hello-world"},"partition":null,"pk_is_handle":false,"schema_version":23,"state":5,"update_timestamp":418360557052624904}')
