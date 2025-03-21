# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_headers_and_sources(storage_s3 .)

add_library(storage_s3 ${storage_s3_headers} ${storage_s3_sources})
target_link_libraries(storage_s3 PUBLIC tiflash_common_io tiflash_contrib::aws_s3 kv_client tipb etcdpb)
