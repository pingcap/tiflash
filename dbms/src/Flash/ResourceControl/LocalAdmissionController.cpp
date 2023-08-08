// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <common/logger_useful.h>
#include <pingcap/kv/Cluster.h>

namespace DB
{

#ifndef DBMS_PUBLIC_GTEST
std::unique_ptr<LocalAdmissionController> LocalAdmissionController::global_instance;
#else
auto LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
#endif

const std::string ResourceGroup::DEFAULT_RESOURCE_GROUP_NAME = "default";
} // namespace DB
