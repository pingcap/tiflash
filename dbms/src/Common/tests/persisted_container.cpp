#include <iomanip>
#include <iostream>

#include <ext/scope_guard.h>

#include <Common/PersistedContainer.h>

int main(int, char **)
{
    {
        std::string file_path = "persisted_container_set_test.dat";
        SCOPE_EXIT({
            Poco::File file(file_path);
            if (file.exists())
                file.remove();
        });
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
        SCOPE_EXIT({
            Poco::File file(file_path);
            if (file.exists())
                file.remove();
        });
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
