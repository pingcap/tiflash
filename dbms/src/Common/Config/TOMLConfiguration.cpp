/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <Common/Config/cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif


#include <Common/Config/TOMLConfiguration.h>
#include <Poco/Exception.h>

#include <sstream>

namespace DB
{

using TOMLBasePtr = std::shared_ptr<cpptoml::base>;

TOMLConfiguration::TOMLConfiguration(TOMLTablePtr toml_doc) : root(toml_doc) { poco_check_ptr(toml_doc); }

bool TOMLConfiguration::getRaw(const std::string & key, std::string & value) const
{
    try
    {
        auto node = root->get_qualified(key);
        if (auto str_node = std::dynamic_pointer_cast<cpptoml::value<std::string>>(node))
        {
            // get raw value without double quote
            value = str_node->get();
        }
        else
        {
            // get as it is in toml even if is table
            std::ostringstream str_out;
            cpptoml::toml_writer writer(str_out);
            node->accept(std::move(writer));
            value = str_out.str();
        }
        return true;
    }
    catch (std::out_of_range)
    {
        return false;
    }
}

bool TOMLConfiguration::find_parent(const std::string & key, TOMLTablePtr & parent, std::string & child_key)
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

void TOMLConfiguration::setRaw(const std::string & key, const std::string & value)
{
    TOMLTablePtr parent;
    std::string child_key;
    if (!find_parent(key, parent, child_key))
        throw Poco::NotFoundException("Key not found in TOML configuration", key);

    parent->erase(child_key);
    parent->insert(child_key, value);
}

void TOMLConfiguration::enumerate(const std::string & key, Keys & range) const
{
    range.clear();

    auto table = key.empty() ? root : root->get_table_qualified(key);
    if (!table)
        return;

    for (auto it = table->begin(); it != table->end(); it++)
        range.push_back(it->first);
}

void TOMLConfiguration::removeRaw(const std::string & key)
{
    TOMLTablePtr parent;
    std::string child_key;

    if (find_parent(key, parent, child_key))
        parent->erase(child_key);
}

} // namespace DB
