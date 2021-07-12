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
    explicit DMConfiguration(std::istream & input) : embeddedChecksum(), debugInfo()
    {
        dtpb::Configuration configuration;
        if (unlikely(!configuration.ParseFromIstream(&input)))
        {
            throw Exception("cannot parse protobuf for DMConfiguration");
        }

        auto                           uncheckedAlgo = configuration.checksum_algorithm();
        std::shared_ptr<B64DigestBase> digest        = nullptr;
        checksumFrameLength                          = configuration.checksum_frame_length();
        switch (uncheckedAlgo)
        {
        case static_cast<uint64_t>(ChecksumAlgo::None):
            digest = std::make_unique<B64Digest<Digest::None>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC32):
            digest = std::make_unique<B64Digest<Digest::CRC32>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC64):
            digest = std::make_unique<B64Digest<Digest::CRC64>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::City128):
            digest = std::make_unique<B64Digest<Digest::City128>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::XXH3):
            digest = std::make_unique<B64Digest<Digest::XXH3>>();
            break;
        default:
            throw Poco::JSON::JSONException("unrecognized checksum algorithm");
        }

        // we cannot directly convert the value to enum;
        // it will be UB if the value is out of the range.
        checksumAlgorithm = static_cast<ChecksumAlgo>(uncheckedAlgo);

        digest->update(&checksumFrameLength, sizeof(checksumFrameLength));
        digest->update(&checksumAlgorithm, sizeof(checksumAlgorithm));

        const auto & embeddedChecksumArray = configuration.embedded_checksum();
        for (const auto & var : embeddedChecksumArray)
        {

            digest->update(var.name().data(), var.name().length());
            digest->update(var.checksum().data(), var.checksum().length());
            embeddedChecksum.emplace(var.name(), var.checksum());
        }

        if (unlikely(!digest->compare(configuration.data_field_checksum().data())))
        {
            throw Poco::JSON::JSONException("data field checksum broken");
        }

        {
            const auto & debugInfoArray = configuration.debug_info();
            for (const auto & var : debugInfoArray)
            {
                debugInfo.emplace(var.name(), var.content());
            }
        }
    }

    explicit DMConfiguration(std::map<std::string, std::string> embeddedChecksum_,
                             size_t                             checksumFrameLength_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
                             ChecksumAlgo                       checksumAlgorithm_   = ChecksumAlgo::XXH3,
                             std::map<std::string, std::string> debugInfo_           = {{"creationCommitHash", TIFLASH_GIT_HASH},
                                                                              {"creationEdition", TIFLASH_EDITION},
                                                                              {"creationVersion", TIFLASH_VERSION},
                                                                              {"creationReleaseVersion", TIFLASH_RELEASE_VERSION},
                                                                              {"creationBuildTime", TIFLASH_UTC_BUILD_TIME}})
        : checksumFrameLength(checksumFrameLength_),
          checksumAlgorithm(checksumAlgorithm_),
          embeddedChecksum(std::move(embeddedChecksum_)),
          debugInfo(std::move(debugInfo_))
    {
    }

    friend std::ostream & operator<<(std::ostream &, const DMConfiguration &);

    [[nodiscard]] size_t                                     getChecksumFrameLength() const { return checksumFrameLength; }
    [[nodiscard]] ChecksumAlgo                               getChecksumAlgorithm() const { return checksumAlgorithm; }
    [[nodiscard]] const std::map<std::string, std::string> & getEmbeddedChecksum() const { return embeddedChecksum; }
    [[nodiscard]] const std::map<std::string, std::string> & getDebugInfo() const { return debugInfo; }

    template <class T>
    void addChecksum(std::string name, T checksum)
    {
        std::ostringstream  tmp;
        Poco::Base64Encoder enc{tmp};
        enc.write(reinterpret_cast<const char *>(&checksum), sizeof(std::decay_t<T>));
        enc.flush();
        embeddedChecksum.template emplace(std::move(name), tmp.str());
    }

private:
    size_t                             checksumFrameLength; // the length of checksum frame
    ChecksumAlgo                       checksumAlgorithm;   // the algorithm of checksum
    std::map<std::string, std::string> embeddedChecksum;    // special checksums for meta files
    std::map<std::string, std::string> debugInfo;           // debugging information
};


inline std::ostream & operator<<(std::ostream & output, const DMConfiguration & config)
{
    dtpb::Configuration            configuration;
    std::unique_ptr<B64DigestBase> digest = nullptr;

    switch (config.checksumAlgorithm)
    {
    case ChecksumAlgo::None:
        digest = std::make_unique<B64Digest<Digest::None>>();
        break;
    case ChecksumAlgo::CRC32:
        digest = std::make_unique<B64Digest<Digest::CRC32>>();
        break;
    case ChecksumAlgo::CRC64:
        digest = std::make_unique<B64Digest<Digest::CRC64>>();
        break;
    case ChecksumAlgo::City128:
        digest = std::make_unique<B64Digest<Digest::City128>>();
        break;
    case ChecksumAlgo::XXH3:
        digest = std::make_unique<B64Digest<Digest::XXH3>>();
        break;
    default:
        throw Poco::JSON::JSONException("unrecognized checksumAlgorithm");
    }

    configuration.set_checksum_algorithm(static_cast<uint64_t>(config.checksumAlgorithm));
    configuration.set_checksum_frame_length(static_cast<uint64_t>(config.checksumFrameLength));
    digest->update(&config.checksumFrameLength, sizeof(config.checksumFrameLength));
    digest->update(&config.checksumAlgorithm, sizeof(config.checksumAlgorithm));

    {
        for (const auto & [name, checksum] : config.embeddedChecksum)
        {
            digest->update(name.data(), name.length());
            digest->update(checksum.data(), checksum.length());
            auto embeddedChecksum = configuration.add_embedded_checksum();
            embeddedChecksum->set_name(name);
            embeddedChecksum->set_checksum(checksum);
        }
    }

    configuration.set_data_field_checksum(digest->raw());

    {
        for (const auto & [name, content] : config.debugInfo)
        {
            auto tmp = configuration.add_debug_info();
            tmp->set_name(name);
            tmp->set_content(content);
        }
    }

    if(!configuration.SerializeToOstream(&output)) {
        throw Exception("unable to serialize protobuf of configuration");
    };

    return output;
};


using DMConfigurationPtr = std::shared_ptr<DMConfiguration>;
} // namespace DB::DM
