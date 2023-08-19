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

#include <IO/ReadHelpers.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>

#include <sstream>


/** Somehow, in case of POST, Poco::Net::HTMLForm doesn't read parameters from URL, only from body.
  * This helper allows to read parameters just from URL.
  */
struct HTMLForm : public Poco::Net::HTMLForm
{
    explicit HTMLForm(const Poco::Net::HTTPRequest & request)
    {
        Poco::URI uri(request.getURI());
        std::istringstream istr(uri.getRawQuery());
        readUrl(istr);
    }

    explicit HTMLForm(const Poco::URI & uri)
    {
        std::istringstream istr(uri.getRawQuery());
        readUrl(istr);
    }


    template <typename T>
    T getParsed(const std::string & key, T default_value)
    {
        auto it = find(key);
        return (it != end()) ? DB::parse<T>(it->second) : default_value;
    }

    template <typename T>
    T getParsed(const std::string & key)
    {
        return DB::parse<T>(get(key));
    }
};
