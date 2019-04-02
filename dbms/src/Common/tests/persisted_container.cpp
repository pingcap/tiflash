#include <iomanip>
#include <iostream>

#include <ext/scope_guard.h>

#include <Common/PersistedContainer.h>

using namespace DB;

int main(int, char **)
{
    auto clear_file = [=](std::string path) {
        Poco::File file(path);
        if (file.exists())
            file.remove();
    };

    {
        std::string file_path = "persisted_container_set_test.dat";
        clear_file(file_path);
        SCOPE_EXIT({ clear_file(file_path); });

        {
            PersistedUnorderedUInt64Set set(file_path);
            set.restore();
            auto & c = set.get();
            c.insert(1);
            c.insert(2);
            c.insert(3);
            set.persist();
        }

        {
            PersistedUnorderedUInt64Set set(file_path);
            set.restore();
            auto & c = set.get();
            for (auto e : c)
            {
                std::cerr << e << std::endl;
            }
            set.persist();
        }
    }

    {
        std::string file_path = "persisted_container_map_test.dat";
        clear_file(file_path);
        SCOPE_EXIT({ clear_file(file_path); });

        {
            PersistedUnorderedUInt64ToStringMap map(file_path);
            map.restore();
            auto & c = map.get();
            c.emplace(1, "1v");
            c.emplace(2, "2v");
            c.emplace(3, "3v");
            map.persist();
        }

        {
            PersistedUnorderedUInt64ToStringMap map(file_path);
            map.restore();
            auto & c = map.get();
            for (auto && [k, v] : c)
            {
                std::cerr << k << ": " << v << std::endl;
            }
            map.persist();
        }
    }

    return 0;
}
