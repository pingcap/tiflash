#pragma once

#include <Core/Names.h>
#include <Core/Types.h>

#include <map>


namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
} // namespace Poco


namespace DB
{
/** Apply substitutions from the macros in config to the string.
  */
class Macros
{
public:
    Macros() = default;
    Macros(const Poco::Util::AbstractConfiguration & config, const String & key);

    /** Replace the substring of the form {macro_name} with the value for macro_name, obtained from the config file.
      * level - the level of recursion.
      */
    String expand(const String & s, size_t level = 0) const;

    /** Apply expand for the list.
      */
    Names expand(const Names & source_names, size_t level = 0) const;

    using MacroMap = std::map<String, String>;
    MacroMap getMacroMap() const { return macros; }

private:
    MacroMap macros;
};


} // namespace DB
