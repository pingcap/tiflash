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

#include <memory>

namespace ext
{
/** Allows to make std::shared_ptr from T with protected constructor.
  *
  * Derive your T class from shared_ptr_helper<T>
  *  and you will have static 'create' method in your class.
  *
  * Downsides:
  * - your class cannot be final;
  * - bad compilation error messages;
  * - bad code navigation.
  * - different dynamic type of created object, you cannot use typeid.
  */
template <typename T>
struct SharedPtrHelper
{
    template <typename... TArgs>
    static auto create(TArgs &&... args)
    {
        /** Local struct makes protected constructor to be accessible by std::make_shared function.
          * This trick is suggested by Yurii Diachenko,
          *  inspired by https://habrahabr.ru/company/mailru/blog/341584/
          *  that is translation of http://videocortex.io/2017/Bestiary/#-voldemort-types
          */
        struct Local : T
        {
            explicit Local(TArgs &&... args)
                : T(std::forward<TArgs>(args)...){};
        };

        return std::make_shared<Local>(std::forward<TArgs>(args)...);
    }
};

} // namespace ext
