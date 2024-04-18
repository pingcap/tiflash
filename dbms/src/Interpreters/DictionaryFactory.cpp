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

#include <Dictionaries/CacheDictionary.h>
#include <Dictionaries/ComplexKeyCacheDictionary.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/TrieDictionary.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
extern const int UNKNOWN_ELEMENT_IN_CONFIG;
extern const int UNSUPPORTED_METHOD;
extern const int TOO_SMALL_BUFFER_SIZE;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes


DictionaryPtr DictionaryFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Context & context)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    const auto & layout_prefix = config_prefix + ".layout";
    config.keys(layout_prefix, keys);
    if (keys.size() != 1)
        throw Exception{
            name + ": element dictionary.layout should have exactly one child element",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

    const DictionaryStructure dict_struct{config, config_prefix + ".structure"};

    auto source_ptr
        = DictionarySourceFactory::instance().create(name, config, config_prefix + ".source", dict_struct, context);

    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

    const auto & layout_type = keys.front();

    if ("complex_key_cache" == layout_type)
    {
        if (!dict_struct.key)
            throw Exception{
                "'key' is required for dictionary of layout 'complex_key_hashed'",
                ErrorCodes::BAD_ARGUMENTS};

        const auto size = config.getInt(layout_prefix + ".complex_key_cache.size_in_cells");
        if (size == 0)
            throw Exception{
                name + ": dictionary of layout 'cache' cannot have 0 cells",
                ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        if (require_nonempty)
            throw Exception{
                name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
                ErrorCodes::BAD_ARGUMENTS};

        return std::make_unique<ComplexKeyCacheDictionary>(
            name,
            dict_struct,
            std::move(source_ptr),
            dict_lifetime,
            size);
    }
    else if ("ip_trie" == layout_type)
    {
        if (!dict_struct.key)
            throw Exception{"'key' is required for dictionary of layout 'ip_trie'", ErrorCodes::BAD_ARGUMENTS};

        // This is specialised trie for storing IPv4 and IPv6 prefixes.
        return std::make_unique<TrieDictionary>(
            name,
            dict_struct,
            std::move(source_ptr),
            dict_lifetime,
            require_nonempty);
    }
    else
    {
        if (dict_struct.key)
            throw Exception{
                "'key' is not supported for dictionary of layout '" + layout_type + "'",
                ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{
                name
                    + ": elements .structure.range_min and .structure.range_max should be defined only "
                      "for a dictionary of layout 'range_hashed'",
                ErrorCodes::BAD_ARGUMENTS};

        if ("cache" == layout_type)
        {
            const auto size = config.getInt(layout_prefix + ".cache.size_in_cells");
            if (size == 0)
                throw Exception{
                    name + ": dictionary of layout 'cache' cannot have 0 cells",
                    ErrorCodes::TOO_SMALL_BUFFER_SIZE};

            if (require_nonempty)
                throw Exception{
                    name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
                    ErrorCodes::BAD_ARGUMENTS};

            return std::make_unique<CacheDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, size);
        }
    }

    throw Exception{name + ": unknown dictionary layout type: " + layout_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
};


} // namespace DB
