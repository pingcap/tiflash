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

#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/ISecurityManager.h>

#include <memory>

namespace DB
{
/** Factory of query engine runtime components / services.
  * Helps to host query engine in external applications
  * by replacing or reconfiguring its components.
  */
class IRuntimeComponentsFactory
{
public:
    virtual std::unique_ptr<ISecurityManager> createSecurityManager() = 0;

    // Repositories with configurations of user-defined objects (dictionaries, models)
    virtual std::unique_ptr<IExternalLoaderConfigRepository> createExternalDictionariesConfigRepository() = 0;

    virtual ~IRuntimeComponentsFactory() {}
};

} // namespace DB
