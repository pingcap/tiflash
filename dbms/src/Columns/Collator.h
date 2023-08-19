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

#include <boost/noncopyable.hpp>
#include <memory>
#include <string>

struct UCollator;

class ICollator : private boost::noncopyable
{
public:
    ICollator() = default;

    virtual ~ICollator() = default;

    virtual int compare(const char * str1, size_t length1, const char * str2, size_t length2) const = 0;

    virtual const std::string & getLocale() const = 0;
};

class Collator : public ICollator
{
public:
    explicit Collator(const std::string & locale_);

    ~Collator() override;

    int compare(const char * str1, size_t length1, const char * str2, size_t length2) const override;

    const std::string & getLocale() const override;

private:
    std::string locale;
    UCollator * collator;
};


using ICollatorPtr = std::shared_ptr<ICollator>;
using CollatorPtr = std::shared_ptr<Collator>;
