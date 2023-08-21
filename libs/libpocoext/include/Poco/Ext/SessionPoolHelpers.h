// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <functional>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Data/SessionPool.h>
#pragma GCC diagnostic pop


using PocoSessionPoolConstructor = std::function<std::shared_ptr<Poco::Data::SessionPool>()>;

/** Is used to adjust max size of default Poco thread pool. See issue #750
  * Acquire the lock, resize pool and construct new Session.
  */
std::shared_ptr<Poco::Data::SessionPool> createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr);
