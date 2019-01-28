#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include <ext/range.h>

#include <Poco/File.h>
#include <Poco/Path.h>

#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

// TODO find a way to unify PersistedContainerSetOrVector and PersistedContainerMap, and Write & Read should be able to use lambda.

template <bool is_set, typename T, template <typename E = T, typename...> class Container, class Write, class Read>
struct PersistedContainerSetOrVector
{
public:
    PersistedContainerSetOrVector(const std::string & path_) : path(path_) {}

    Container<T> & get() { return container; }

    void persist()
    {
        std::string tmp_file_path = path + ".tmp." + DB::toString(Poco::Timestamp().epochMicroseconds());
        DB::WriteBufferFromFile file_buf(tmp_file_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);
        size_t size = container.size();
        writeIntBinary(size, file_buf);
        for (const T & t : container)
        {
            write(t, file_buf);
        }
        file_buf.next();
        file_buf.sync();

        Poco::File(tmp_file_path).renameTo(path);
    }

    void restore()
    {
        if (!Poco::File(path).exists())
            return;
        DB::ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        size_t size;
        readIntBinary(size, file_buf);
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (is_set)
            {
                container.insert(std::move(read(file_buf)));
            }
            else
            {
                container.push_back(std::move(read(file_buf)));
            }
        }
    }

    void drop()
    {
        Poco::File f(path);
        if (f.exists())
            f.remove(false);
        Container<T> tmp;
        container.swap(tmp);
    }

private:
    std::string path;
    Container<T> container;
    Write write{};
    Read read{};
};

template <typename Key,
          typename Value,
          template <typename CKey = Key, typename CValue = Value, typename...> class Container,
          class Write,
          class Read>
struct PersistedContainerMap
{
public:
    PersistedContainerMap(const std::string & path_) : path(path_) {}

    Container<Key, Value> & get() { return container; }

    void persist()
    {
        std::string tmp_file_path = path + ".tmp." + DB::toString(Poco::Timestamp().epochMicroseconds());
        DB::WriteBufferFromFile file_buf(tmp_file_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);
        size_t size = container.size();
        writeIntBinary(size, file_buf);
        for (auto && [k, v] : container)
        {
            write(k, v, file_buf);
        }
        file_buf.next();
        file_buf.sync();

        Poco::File(tmp_file_path).renameTo(path);
    }

    void restore()
    {
        if (!Poco::File(path).exists())
            return;
        DB::ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);
        size_t size;
        readIntBinary(size, file_buf);
        for (size_t i = 0; i < size; ++i)
        {
            const auto && [k, v] = read(file_buf);
            container.emplace(k, v);
        }
    }

    void drop()
    {
        Poco::File f(path);
        if (f.exists())
            f.remove(false);
        Container<Key, Value> tmp;
        container.swap(tmp);
    }

private:
    std::string path;
    Container<Key, Value> container;
    Write write{};
    Read read{};
};

template <typename T, template <typename E = T, typename...> class Container, class Write, class Read>
using PersistedContainerSet = PersistedContainerSetOrVector<true, T, Container, Write, Read>;

template <typename T, template <typename E = T, typename...> class Container, class Write, class Read>
using PersistedContainerVector = PersistedContainerSetOrVector<false, T, Container, Write, Read>;

struct UInt64Write
{
    void operator()(UInt64 v, DB::WriteBuffer & buf) { writeIntBinary(v, buf); }
};
struct UInt64Read
{
    UInt64 operator()(DB::ReadBuffer & buf)
    {
        UInt64 v;
        readIntBinary(v, buf);
        return v;
    }
};
using PersistedUnorderedUInt64Set = PersistedContainerSet<UInt64, std::unordered_set, UInt64Write, UInt64Read>;

struct UInt64StringWrite
{
    void operator()(UInt64 k, const std::string & v, DB::WriteBuffer & buf)
    {
        writeIntBinary(k, buf);
        writeStringBinary(v, buf);
    }
};
struct UInt64StringRead
{
    std::pair<UInt64, std::string> operator()(DB::ReadBuffer & buf)
    {
        UInt64 k;
        readIntBinary(k, buf);
        std::string v;
        readStringBinary(v, buf);
        return {k, v};
    }
};
using PersistedUnorderedUInt64ToStringMap
    = PersistedContainerMap<UInt64, std::string, std::unordered_map, UInt64StringWrite, UInt64StringRead>;
