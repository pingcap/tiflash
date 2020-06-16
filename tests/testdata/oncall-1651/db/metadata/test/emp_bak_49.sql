ATTACH TABLE emp_bak_49
(
    id Int32, 
    fname String, 
    lname String, 
    store_id Int32, 
    department_id Int32
)
ENGINE = DeltaMerge(id, '{"belonging_table_id":48,"cols":[{"comment":"","default":null,"id":1,"name":{"L":"id","O":"id"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":515,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":2,"name":{"L":"fname","O":"fname"},"offset":1,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4097,"Flen":25,"Tp":15}},{"comment":"","default":null,"id":3,"name":{"L":"lname","O":"lname"},"offset":2,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4097,"Flen":25,"Tp":15}},{"comment":"","default":null,"id":4,"name":{"L":"store_id","O":"store_id"},"offset":3,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4097,"Flen":11,"Tp":3}},{"comment":"","default":null,"id":5,"name":{"L":"department_id","O":"department_id"},"offset":4,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":4097,"Flen":11,"Tp":3}}],"comment":"","id":49,"is_partition_sub_table":true,"name":{"L":"employees_49","O":"employees_49"},"partition":{"definitions":[{"comment":"","id":49,"name":{"L":"p0","O":"p0"}},{"comment":"","id":50,"name":{"L":"p1","O":"p1"}},{"comment":"","id":51,"name":{"L":"p2","O":"p2"}},{"comment":"","id":52,"name":{"L":"p3","O":"p3"}}],"enable":true,"expr":"`id`","num":4,"type":1},"pk_is_handle":true,"schema_version":25,"state":5,"update_timestamp":417160265315647498}')
