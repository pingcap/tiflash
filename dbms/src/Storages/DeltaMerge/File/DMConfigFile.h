#pragma once
#include <Common/config_version.h>
#include <Poco/DynamicStruct.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Storages/DeltaMerge/File/Checksum/Checksum.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <dmfile.pb.h>
#pragma GCC diagnostic pop

#include <map>
#include <string>

namespace DB::DM
{


class DMConfiguration
{
public:
    explicit DMConfiguration(std::istream & input) : embedded_checksum(), debug_info()
    {
        dtpb::Configuration configuration;
        if (unlikely(!configuration.ParseFromIstream(&input)))
        {
            throw Exception("cannot parse protobuf for DMConfiguration");
        }

        auto                 unchecked_algorithm = configuration.checksum_algorithm();
        UnifiedDigestBaseBox digest              = nullptr;
        checksum_frame_length                    = configuration.checksum_frame_length();
        switch (unchecked_algorithm)
        {
        case static_cast<uint64_t>(ChecksumAlgo::None):
            digest = std::make_unique<UnifiedDigest<Digest::None>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC32):
            digest = std::make_unique<UnifiedDigest<Digest::CRC32>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC64):
            digest = std::make_unique<UnifiedDigest<Digest::CRC64>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::City128):
            digest = std::make_unique<UnifiedDigest<Digest::City128>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::XXH3):
            digest = std::make_unique<UnifiedDigest<Digest::XXH3>>();
            break;
        default:
            throw Exception("unrecognized checksum algorithm");
        }

        // we cannot directly convert the value to enum;
        // it will be UB if the value is out of the range.
        checksum_algorihm = static_cast<ChecksumAlgo>(unchecked_algorithm);

        digest->update(&checksum_frame_length, sizeof(checksum_frame_length));
        digest->update(&checksum_algorihm, sizeof(checksum_algorihm));

        const auto & embedded_checksum_array = configuration.embedded_checksum();
        for (const auto & var : embedded_checksum_array)
        {

            digest->update(var.name().data(), var.name().length());
            digest->update(var.checksum().data(), var.checksum().length());
            embedded_checksum.emplace(var.name(), var.checksum());
        }

        if (unlikely(!digest->compareRaw(configuration.data_field_checksum())))
        {
            throw Exception("data field checksum broken");
        }

        {
            const auto & debugInfoArray = configuration.debug_info();
            for (const auto & var : debugInfoArray)
            {
                debug_info.emplace(var.name(), var.content());
            }
        }
    }

    explicit DMConfiguration(std::map<std::string, std::string> embedded_checksum_     = {},
                             size_t                             checksum_frame_length_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
                             ChecksumAlgo                       checksum_algorithm_    = ChecksumAlgo::XXH3,
                             std::map<std::string, std::string> debug_info_            = {{"creation_commit_hash", TIFLASH_GIT_HASH},
                                                                               {"creation_edition", TIFLASH_EDITION},
                                                                               {"creation_version", TIFLASH_VERSION},
                                                                               {"creation_release_version", TIFLASH_RELEASE_VERSION},
                                                                               {"creation_build_time", TIFLASH_UTC_BUILD_TIME}})
        : checksum_frame_length(checksum_frame_length_),
          checksum_algorihm(checksum_algorithm_),
          embedded_checksum(std::move(embedded_checksum_)),
          debug_info(std::move(debug_info_))
    {
    }

    friend std::ostream & operator<<(std::ostream &, const DMConfiguration &);

    [[nodiscard]] size_t getChecksumFrameLength() const { return checksum_frame_length; }
    [[nodiscard]] size_t getChecksumHeaderLength() const
    {
        switch (checksum_algorihm)
        {
        case ChecksumAlgo::None:
            return sizeof(ChecksumFrame<Digest::None>);
        case ChecksumAlgo::CRC32:
            return sizeof(ChecksumFrame<Digest::CRC32>);
        case ChecksumAlgo::CRC64:
            return sizeof(ChecksumFrame<Digest::CRC64>);
        case ChecksumAlgo::City128:
            return sizeof(ChecksumFrame<Digest::City128>);
        case ChecksumAlgo::XXH3:
            return sizeof(ChecksumFrame<Digest::XXH3>);
        }
        return 0;
    }
    [[nodiscard]] ChecksumAlgo                               getChecksumAlgorithm() const { return checksum_algorihm; }
    [[nodiscard]] std::map<std::string, std::string> &       getEmbeddedChecksum() { return embedded_checksum; }
    [[nodiscard]] const std::map<std::string, std::string> & getDebugInfo() const { return debug_info; }

    void addChecksum(std::string name, std::string value) { embedded_checksum.template emplace(std::move(name), std::move(value)); }

    [[nodiscard]] UnifiedDigestBaseBox createUnifiedDigest() const
    {
        switch (checksum_algorihm)
        {
        case ChecksumAlgo::None:
            return std::make_unique<UnifiedDigest<Digest::None>>();
        case ChecksumAlgo::CRC32:
            return std::make_unique<UnifiedDigest<Digest::CRC32>>();
        case ChecksumAlgo::CRC64:
            return std::make_unique<UnifiedDigest<Digest::CRC64>>();
        case ChecksumAlgo::City128:
            return std::make_unique<UnifiedDigest<Digest::City128>>();
        case ChecksumAlgo::XXH3:
            return std::make_unique<UnifiedDigest<Digest::XXH3>>();
        default:
            throw Exception("unrecognized checksum_algorihm");
        }
    }

private:
    size_t                             checksum_frame_length; // the length of checksum frame
    ChecksumAlgo                       checksum_algorihm;     // the algorithm of checksum
    std::map<std::string, std::string> embedded_checksum;     // special checksums for meta files
    std::map<std::string, std::string> debug_info;            // debugging information
};


inline std::ostream & operator<<(std::ostream & output, const DMConfiguration & config)
{
    dtpb::Configuration  configuration;
    UnifiedDigestBaseBox digest = config.createUnifiedDigest();

    configuration.set_checksum_algorithm(static_cast<uint64_t>(config.checksum_algorihm));
    configuration.set_checksum_frame_length(static_cast<uint64_t>(config.checksum_frame_length));
    digest->update(&config.checksum_frame_length, sizeof(config.checksum_frame_length));
    digest->update(&config.checksum_algorihm, sizeof(config.checksum_algorihm));

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
        throw Exception("unable to serialize protobuf of configuration");
    };

    return output;
};


using DMConfigurationPtr = std::shared_ptr<DMConfiguration>;
} // namespace DB::DM
