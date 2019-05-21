#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include <ext/range.h>

#include <Poco/File.h>
#include <Poco/Path.h>

#include <Core/Types.h>
#include <IO/HashingReadBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CHECKSUM_DOESNT_MATCH;
}

constexpr UInt8 PERSISTED_CONTAINER_MAGIC_WORD = 0xFF;
constexpr size_t HASH_CODE_LENGTH = sizeof(DB::HashingWriteBuffer::uint128);

template <bool is_map, bool is_set, class Trait>
struct PersistedContainer : public Trait
{
public:
    using Container = typename Trait::Container;
    using Write = typename Trait::Write;
    using Read = typename Trait::Read;

    explicit PersistedContainer(const std::string & path_) : path(path_) {}

    auto & get() { return container; }
    const auto & get() const { return container; }

    void persist()
    {
        std::string tmp_file_path = path + ".tmp." + DB::toString(Poco::Timestamp().epochMicroseconds());
        {
            DB::WriteBufferFromFile file_buf(tmp_file_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);
            file_buf.seek(HASH_CODE_LENGTH);

            DB::HashingWriteBuffer hash_buf(file_buf);
            size_t size = container.size();
            writeIntBinary(size, hash_buf);
            if constexpr (is_map)
            {
                for (auto && [k, v] : container)
                {
                    writeIntBinary(PERSISTED_CONTAINER_MAGIC_WORD, hash_buf);
                    write(k, v, hash_buf);
                }
            }
            else
            {
                for (const auto & t : container)
                {
                    writeIntBinary(PERSISTED_CONTAINER_MAGIC_WORD, hash_buf);
                    write(t, hash_buf);
                }
            }
            hash_buf.next();

            auto hashcode = hash_buf.getHash();
            file_buf.seek(0);
            writeIntBinary(hashcode.first, file_buf);
            writeIntBinary(hashcode.second, file_buf);

            file_buf.sync();
        }
        Poco::File(tmp_file_path).renameTo(path);
    }

    void restore()
    {
        if (!Poco::File(path).exists())
            return;
        DB::ReadBufferFromFile file_buf(path, DBMS_DEFAULT_BUFFER_SIZE, O_RDONLY);

        DB::HashingReadBuffer::uint128 expected_hashcode;
        readIntBinary(expected_hashcode.first, file_buf);
        readIntBinary(expected_hashcode.second, file_buf);

        DB::HashingReadBuffer hash_buf(file_buf);
        size_t size;
        readIntBinary(size, hash_buf);
        UInt8 word;
        for (size_t i = 0; i < size; ++i)
        {
            readIntBinary(word, hash_buf);
            if (word != PERSISTED_CONTAINER_MAGIC_WORD)
                throw DB::Exception("Magic word does not match!", DB::ErrorCodes::CHECKSUM_DOESNT_MATCH);

            if constexpr (is_map)
            {
                const auto && [k, v] = read(hash_buf);
                container.emplace(k, v);
            }
            else if constexpr (is_set)
            {
                container.insert(std::move(read(hash_buf)));
            }
            else
            {
                container.push_back(std::move(read(hash_buf)));
            }
        }
        auto hashcode = hash_buf.getHash();
        if (hashcode != expected_hashcode)
            throw DB::Exception("Hashcode does not match!", DB::ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    void drop()
    {
        Poco::File f(path);
        if (f.exists())
            f.remove(false);
        Container tmp;
        container.swap(tmp);
    }

private:
    std::string path;
    Container container;
    Write write{};
    Read read{};
};

template <typename K, typename V, template <typename...> class C, typename W, typename R>
struct MapTrait
{
    using Container = C<K, V>;
    using Write = W;
    using Read = R;
};

template <typename T, template <typename...> class C, typename W, typename R>
struct VecSetTrait
{
    using Container = C<T>;
    using Write = W;
    using Read = R;
};

template <typename K, typename V, template <typename...> class C, class Write, class Read>
using PersistedContainerMap = PersistedContainer<true, false, MapTrait<K, V, C, Write, Read>>;

template <typename T, template <typename...> class C, class Write, class Read>
using PersistedContainerSet = PersistedContainer<false, true, VecSetTrait<T, C, Write, Read>>;

template <typename T, template <typename...> class C, class Write, class Read>
using PersistedContainerVector = PersistedContainer<false, false, VecSetTrait<T, C, Write, Read>>;

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

} // namespace DB
