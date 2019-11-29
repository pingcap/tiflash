#pragma once


#include "Poco/Util/MapConfiguration.h"
#include "Poco/DOM/AutoPtr.h"
#include <istream>
#include <Common/Config/cpptoml.h>

namespace DB
{

using TOMLTablePtr = std::shared_ptr<cpptoml::table>;

class TOMLConfiguration : public Poco::Util::AbstractConfiguration
{
public:
    TOMLConfiguration(TOMLTablePtr toml_doc);

protected:
    bool getRaw(const std::string& key, std::string& value) const;
    /// If the property with the given key exists, stores the property's value
    /// in value and returns true. Otherwise, returns false.
    ///
    /// Must be overridden by subclasses.

    void setRaw(const std::string& key, const std::string& value);
    /// Sets the property with the given key to the given value.
    /// An already existing value for the key is overwritten.
    ///
    /// Must be overridden by subclasses.

    void enumerate(const std::string& key, Keys& range) const;
    /// Returns in range the names of all subkeys under the given key.
    /// If an empty key is passed, all root level keys are returned.

    void removeRaw(const std::string& key);
private:
    TOMLTablePtr root;
};

}

