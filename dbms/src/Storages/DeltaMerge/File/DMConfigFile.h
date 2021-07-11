#pragma once
#include <Poco/DynamicStruct.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Storages/DeltaMerge/File/Checksum/Checksum.h>

#include <map>
#include <string>

namespace DB::DM
{


class DMConfiguration
{
public:
    explicit DMConfiguration(std::istream & input, bool loadDebugInfo = true) : embeddedChecksum(), debugInfo()
    {
        Poco::JSON::Parser             parser            = {};
        auto                           configVar         = parser.parse(input);
        auto                           configObj         = configVar.extract<Poco::JSON::Object::Ptr>();
        auto                           uncheckedAlgo     = configObj->getValue<uint64_t>("checksumAlgorithm");
        auto                           dataFieldChecksum = configObj->getValue<std::string>("dataFieldChecksum");
        std::shared_ptr<B64DigestBase> digest            = nullptr;
        checksumFrameLength                              = configObj->getValue<size_t>("checksumFrameLength");
        switch (uncheckedAlgo)
        {
        case static_cast<uint64_t>(ChecksumAlgo::None):
            digest = std::make_shared<B64Digest<Digest::None>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC32):
            digest = std::make_shared<B64Digest<Digest::CRC32>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::CRC64):
            digest = std::make_shared<B64Digest<Digest::CRC64>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::City128):
            digest = std::make_shared<B64Digest<Digest::City128>>();
            break;
        case static_cast<uint64_t>(ChecksumAlgo::XXH3):
            digest = std::make_shared<B64Digest<Digest::XXH3>>();
            break;
        default:
            throw Poco::JSON::JSONException("unrecognized checksumAlgorithm");
        }

        // we cannot directly convert the value to enum;
        // it will be UB if the value is out of the range.
        checksumAlgorithm = static_cast<ChecksumAlgo>(uncheckedAlgo);

        digest->update(&checksumFrameLength, sizeof(checksumFrameLength));
        digest->update(&checksumAlgorithm, sizeof(checksumAlgorithm));

        auto embeddedChecksumArray = configObj->getArray("embeddedChecksum");
        for (const auto & var : *embeddedChecksumArray)
        {
            auto obj      = var.extract<Poco::JSON::Object::Ptr>();
            auto name     = obj->getValue<std::string>("name");
            auto checksum = obj->getValue<std::string>("checksum");
            digest->update(name.data(), name.length());
            digest->update(checksum.data(), checksum.length());
            embeddedChecksum.emplace(std::move(name), std::move(checksum));
        }

        if (unlikely(!digest->compare(dataFieldChecksum)))
        {
            throw Poco::JSON::JSONException("data field checksum broken");
        }

        if (loadDebugInfo)
        {
            auto debugInfoArray = configObj->getArray("debugInfo");
            for (const auto & var : *debugInfoArray)
            {
                auto obj     = var.extract<Poco::JSON::Object::Ptr>();
                auto name    = obj->getValue<std::string>("name");
                auto content = obj->getValue<std::string>("content");
                debugInfo.emplace(std::move(name), std::move(content));
            }
        }
    }

    DMConfiguration(size_t                             checksumFrameLength_,
                    ChecksumAlgo                       checksumAlgorithm_,
                    std::map<std::string, std::string> embeddedChecksum_,
                    std::map<std::string, std::string> debugInfo_)
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

private:
    size_t                             checksumFrameLength; // the length of checksum frame
    ChecksumAlgo                       checksumAlgorithm;   // the algorithm of checksum
    std::map<std::string, std::string> embeddedChecksum;    // special checksums for meta files
    std::map<std::string, std::string> debugInfo;           // debugging information
};


inline std::ostream & operator<<(std::ostream & output, const DMConfiguration & config)
{
    auto                           obj    = Poco::JSON::Object{};
    std::shared_ptr<B64DigestBase> digest = nullptr;

    switch (config.checksumAlgorithm)
    {
    case ChecksumAlgo::None:
        digest = std::make_shared<B64Digest<Digest::None>>();
        break;
    case ChecksumAlgo::CRC32:
        digest = std::make_shared<B64Digest<Digest::CRC32>>();
        break;
    case ChecksumAlgo::CRC64:
        digest = std::make_shared<B64Digest<Digest::CRC64>>();
        break;
    case ChecksumAlgo::City128:
        digest = std::make_shared<B64Digest<Digest::City128>>();
        break;
    case ChecksumAlgo::XXH3:
        digest = std::make_shared<B64Digest<Digest::XXH3>>();
        break;
    default:
        throw Poco::JSON::JSONException("unrecognized checksumAlgorithm");
    }

    obj.set("checksumAlgorithm", static_cast<uint64_t>(config.checksumAlgorithm));
    obj.set("checksumFrameLength", config.checksumFrameLength);
    digest->update(&config.checksumFrameLength, sizeof(config.checksumFrameLength));
    digest->update(&config.checksumAlgorithm, sizeof(config.checksumAlgorithm));

    {
        auto embeddedChecksumArray = Poco::Dynamic::Array{};
        for (const auto & [name, checksum] : config.embeddedChecksum)
        {
            digest->update(name.data(), name.length());
            digest->update(checksum.data(), checksum.length());
            auto tmp = Poco::JSON::Object{};
            tmp.set("name", name);
            tmp.set("checksum", checksum);
            embeddedChecksumArray.emplace_back(tmp);
        }
        obj.set("embeddedChecksum", embeddedChecksumArray); // TODO: maybe should move? but Poco is being silly here.
    }

    obj.set("dataFieldChecksum", digest->base64());

    {
        auto debugInfoArray = Poco::Dynamic::Array{};
        for (const auto & [name, content] : config.debugInfo)
        {
            auto tmp = Poco::JSON::Object{};
            tmp.set("name", name);
            tmp.set("content", content);
            debugInfoArray.emplace_back(tmp);
        }
        obj.set("debugInfo", debugInfoArray); // TODO: maybe should move? but Poco is being silly here.
    }

    obj.stringify(output);

    return output;
};

} // namespace DB::DM
