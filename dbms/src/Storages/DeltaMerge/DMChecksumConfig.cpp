#include <Common/TiFlashException.h>
#include <IO/ChecksumBuffer.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <dmfile.pb.h>
#pragma GCC diagnostic pop

#include <Storages/DeltaMerge/DMChecksumConfig.h>

namespace DB::DM
{
DMChecksumConfig::DMChecksumConfig(std::istream & input)
    : embedded_checksum()
    , debug_info()
{
    dtpb::ChecksumConfig configuration;
    if (unlikely(!configuration.ParseFromIstream(&input)))
    {
        throw TiFlashException("failed to parse configuration proto from input stream", Errors::Checksum::IOFailure);
    }

    auto unchecked_algorithm = configuration.checksum_algorithm();
    DB::UnifiedDigestBaseBox digest = nullptr;
    checksum_frame_length = configuration.checksum_frame_length();
    switch (unchecked_algorithm)
    {
    case static_cast<uint64_t>(DB::ChecksumAlgo::None):
        digest = std::make_unique<DB::UnifiedDigest<DB::Digest::None>>();
        break;
    case static_cast<uint64_t>(DB::ChecksumAlgo::CRC32):
        digest = std::make_unique<DB::UnifiedDigest<DB::Digest::CRC32>>();
        break;
    case static_cast<uint64_t>(DB::ChecksumAlgo::CRC64):
        digest = std::make_unique<DB::UnifiedDigest<DB::Digest::CRC64>>();
        break;
    case static_cast<uint64_t>(DB::ChecksumAlgo::City128):
        digest = std::make_unique<DB::UnifiedDigest<DB::Digest::City128>>();
        break;
    case static_cast<uint64_t>(DB::ChecksumAlgo::XXH3):
        digest = std::make_unique<DB::UnifiedDigest<DB::Digest::XXH3>>();
        break;
    default:
        throw TiFlashException("unrecognized checksum algorithm", Errors::Checksum::Internal);
    }

    // we cannot directly convert the value to enum;
    // it will be UB if the value is out of the range.
    checksum_algorithm = static_cast<DB::ChecksumAlgo>(unchecked_algorithm);

    digest->update(&checksum_frame_length, sizeof(checksum_frame_length));
    digest->update(&checksum_algorithm, sizeof(checksum_algorithm));

    const auto & embedded_checksum_array = configuration.embedded_checksum();
    for (const auto & var : embedded_checksum_array)
    {
        digest->update(var.name().data(), var.name().length());
        digest->update(var.checksum().data(), var.checksum().length());
        embedded_checksum.emplace(var.name(), var.checksum());
    }

    if (unlikely(!digest->compareRaw(configuration.data_field_checksum())))
    {
        throw TiFlashException("critical fields of configuration corrupted", Errors::Checksum::DataCorruption);
    }

    {
        const auto & debug_info_array = configuration.debug_info();
        for (const auto & var : debug_info_array)
        {
            debug_info.emplace(var.name(), var.content());
        }
    }
}

std::ostream & operator<<(std::ostream & output, const DMChecksumConfig & config)
{
    dtpb::ChecksumConfig configuration;
    DB::UnifiedDigestBaseBox digest = config.createUnifiedDigest();

    configuration.set_checksum_algorithm(static_cast<uint64_t>(config.checksum_algorithm));
    configuration.set_checksum_frame_length(static_cast<uint64_t>(config.checksum_frame_length));
    digest->update(&config.checksum_frame_length, sizeof(config.checksum_frame_length));
    digest->update(&config.checksum_algorithm, sizeof(config.checksum_algorithm));

    {
        for (const auto & [name, checksum] : config.embedded_checksum)
        {
            digest->update(name.data(), name.length());
            digest->update(checksum.data(), checksum.length());
            auto embedded_checksum = configuration.add_embedded_checksum();
            embedded_checksum->set_name(name);
            embedded_checksum->set_checksum(checksum);
        }
    }

    configuration.set_data_field_checksum(digest->raw());

    {
        for (const auto & [name, content] : config.debug_info)
        {
            auto tmp = configuration.add_debug_info();
            tmp->set_name(name);
            tmp->set_content(content);
        }
    }

    if (!configuration.SerializeToOstream(&output))
    {
        throw TiFlashException("failed to output configuration proto to stream", Errors::Checksum::IOFailure);
    };

    return output;
};


} // namespace DB::DM
