#pragma once

#include <istream>

#include "Poco/DOM/AutoPtr.h"
#include "Poco/Util/MapConfiguration.h"

namespace cpptoml
{
class table;
}
namespace DB
{

using TOMLTablePtr = std::shared_ptr<cpptoml::table>;

class TOMLConfiguration : public Poco::Util::AbstractConfiguration
{
public:
    TOMLConfiguration(TOMLTablePtr toml_doc);

protected:
    bool getRaw(const std::string & key, std::string & value) const;
    /// If the property with the given key exists, stores the property's value
    /// in value and returns true. Otherwise, returns false.

    void setRaw(const std::string & key, const std::string & value);
    /// Sets the property with the given key to the given value.
    /// An already existing value for the key is overwritten.

    void enumerate(const std::string & key, Keys & range) const;
    /// Returns in range the names of all subkeys under the given key.
    /// If an empty key is passed, all root level keys are returned.

    void removeRaw(const std::string & key);
    /// Removes the property with the given key.
    /// Does nothing if the key does not exist.
private:
    TOMLTablePtr root;
    bool find_parent(const std::string & key, TOMLTablePtr & parent, std::string & child_key);
};

} // namespace DB
