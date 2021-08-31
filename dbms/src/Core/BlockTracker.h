#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromFile.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <thread>

namespace DB
{
class BlockTracker
{
public:
    BlockTracker(const String & name_)
        : name(name_)
    {}

    String getDataFileName() const
    {
        size_t thread_id_hash = std::hash<std::thread::id>()(std::this_thread::get_id());
        return fmt::format("{}-{}.data", name, thread_id_hash);
    }

    void track(const Block & block)
    {
        LOG_DEBUG(&Poco::Logger::get("BlockTracker"), fmt::format("{}: {}", name, block.dumpStructure()));

        if (block.rows() == 0)
            return;

        if (!writer)
        {
            auto header = block.cloneEmpty();
            writer = std::make_unique<Writer>(getDataFileName(), header);
        }

        writer->write(block);
    }

private:
    struct Writer
    {
        WriteBufferFromFile buffer;
        NativeBlockOutputStream stream;

        Writer(const String & path, const Block & header)
            : buffer(path)
            , stream(buffer, 0, header)
        {
            stream.writePrefix();
        }

        ~Writer()
        {
            stream.writeSuffix();
            stream.flush();
            buffer.sync();
            buffer.close();
        }

        void write(const Block & block)
        {
            stream.write(block);
        }
    };

    String name;
    std::unique_ptr<Writer> writer;
};

} // namespace DB
