-- Copyright 2022 PingCAP, Ltd.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

ATTACH TABLE abc
(
    pk Nullable(Int32), 
    _tidb_rowid Int64
)
ENGINE = DeltaMerge(_tidb_rowid, '{"cols":[{"comment":"","default":null,"id":1,"name":{"L":"pk","O":"pk"},"offset":0,"origin_default":null,"state":5,"type":{"Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":45,"name":{"L":"aaa","O":"aaa"},"partition":null,"pk_is_handle":false,"schema_version":23,"state":5,"update_timestamp":417160204721061891}')
