-- Copyright 2022 PingCAP, Inc.
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

ATTACH TABLE t_45
(
    a2 Int32
)
ENGINE = DeltaMerge(a, '{"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a2","O":"a2"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":3,"Flen":11,"Tp":3}}],"comment":"","id":45,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":true,"schema_version":24,"state":5,"update_timestamp":419173445783257096}', 0)
