#include "TOMLConfiguration.h"
#include "Poco/Exception.h"

namespace DB
{

using TOMLBasePtr = std::shared_ptr<cpptoml::base>;

TOMLConfiguration::TOMLConfiguration(TOMLTablePtr toml_doc): root(toml_doc)
{
    poco_check_ptr (toml_doc);
}

bool TOMLConfiguration::getRaw(const std::string& key, std::string& value) const
{
    cpptoml::option<std::string> opt_value = root->get_qualified_as<std::string>(key);
    if (opt_value)
    {
        value = *opt_value;
        return true;
    }

    return false;
}

bool TOMLConfiguration::find_parent(const std::string key, TOMLTablePtr & parent, std::string & child_key)
{
    auto pos = key.find_last_of('.');

    if (pos == 0 || pos >= key.size() - 1)
        return false;

    if (pos != std::string::npos)
    {
        // parent should be a table
        auto parent_key = key.substr(0, pos);
        auto res = root->get_table_qualified(key);
        if (!res)
            return false;

        parent = res;
        child_key = key.substr(pos + 1);
        return true;
    }
    else
    {
        // root table
        if (!root->contains(key))
            return false;

        parent = root;
        child_key = key;
        return true;
    }
}

void TOMLConfiguration::setRaw(const std::string& key, const std::string& value)
{
    TOMLTablePtr parent;
    std::string child_key;
    if (!find_parent(key, parent, child_key))
        throw Poco::NotFoundException("Key not found in TOML configuration", key);

    parent->erase(child_key);
    parent->insert(child_key, value);
}

void TOMLConfiguration::enumerate(const std::string& key, Keys& range) const
{
    range.clear();

    auto table = key.empty() ? root : root->get_table_qualified(key);
    if (!table)
        return;

    for (auto it = table->begin(); it != table->end(); it++)
        range.push_back(it->first);
}

void TOMLConfiguration::removeRaw(const std::string& key)
{
    TOMLTablePtr parent;
    std::string child_key;

    if (find_parent(key, parent, child_key))
        parent->erase(child_key);
}

}