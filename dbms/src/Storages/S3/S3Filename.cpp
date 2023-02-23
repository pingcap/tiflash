// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Exception.h>
#include <Common/StringUtils/StringRefUtils.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/S3/S3Filename.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <magic_enum.hpp>
#include <string_view>

namespace DB::S3
{

//==== Serialize/Deserialize ====//

namespace details
{

/// parsing LockFile
const static re2::RE2 rgx_lock("^lock/s(?P<store_id>[0-9]+)/(?P<data_subpath>.+)$");
const static re2::RE2 rgx_lock_suffix(".lock_s(?P<lock_store_id>[0-9]+)_(?P<lock_seq>[0-9]+)");

/// parsing CheckpointManifest or DataFile
const static re2::RE2 rgx_store_prefix("^s(?P<store_id>[0-9]+)/$");
const static re2::RE2 rgx_data_or_manifest("^s(?P<store_id>[0-9]+)/(data|manifest)/(?P<data_subpath>.+)$");
const static re2::RE2 rgx_subpath_manifest("mf_(?P<upload_seq>[0-9]+)");
constexpr static std::string_view DELMARK_SUFFIX = ".del";

String toFullKey(const S3FilenameType type, const StoreID store_id, const std::string_view data_subpath)
{
    switch (type)
    {
    case S3FilenameType::DataFile:
        return fmt::format("s{}/data/{}", store_id, data_subpath);
    case S3FilenameType::CheckpointManifest:
        return fmt::format("s{}/manifest/{}", store_id, data_subpath);
    case S3FilenameType::StorePrefix:
        return fmt::format("s{}/", store_id);
    default:
        throw Exception(fmt::format("Not support type! type={}", magic_enum::enum_name(type)));
    }
    __builtin_unreachable();
}

} // namespace details

String S3FilenameView::toFullKey() const
{
    return details::toFullKey(type, store_id, data_subpath);
}

String S3Filename::toFullKey() const
{
    return details::toFullKey(type, store_id, data_subpath);
}

String S3Filename::toManifestPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return details::toFullKey(type, store_id, data_subpath) + "manifest/";
}

String S3Filename::toDataPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return details::toFullKey(type, store_id, data_subpath) + "data/";
}

String S3Filename::getLockPrefix()
{
    return "lock/";
}

S3FilenameView S3FilenameView::fromKey(const std::string_view fullpath)
{
    S3FilenameView res{.type = S3FilenameType::Invalid};
    re2::StringPiece fullpath_sp{fullpath.data(), fullpath.size()};
    re2::StringPiece type_view, datafile_path;
    // lock/s${store_id}/${data_subpath}.lock_s${lock_store_id}_${lock_seq}
    if (startsWith(fullpath, "lock/"))
    {
        if (!re2::RE2::FullMatch(fullpath_sp, details::rgx_lock, &res.store_id, &datafile_path))
            return res;

        const auto lock_start_npos = datafile_path.find(".lock_");
        if (lock_start_npos == re2::StringPiece::npos)
        {
            res.type = S3FilenameType::Invalid;
            return res;
        }

        // ${data_subpath}.lock_s${lock_store_id}_${lock_seq}
        if (datafile_path.starts_with("dat_") || datafile_path.starts_with("t_") || datafile_path.starts_with("ks_"))
            res.type = S3FilenameType::LockFile;
        else
        {
            res.type = S3FilenameType::Invalid;
            return res;
        }
        // .lock_s${lock_store_id}_${lock_seq}
        res.lock_suffix = std::string_view(datafile_path.begin() + lock_start_npos, datafile_path.size() - lock_start_npos);
        datafile_path.remove_suffix(res.lock_suffix.size());
        res.data_subpath = std::string_view(datafile_path.data(), datafile_path.size());
        return res;
    }

    if (!re2::RE2::FullMatch(fullpath_sp, details::rgx_data_or_manifest, &res.store_id, &type_view, &datafile_path))
        return res; // invalid

    if (type_view == "manifest")
        res.type = S3FilenameType::CheckpointManifest;
    else if (type_view == "data")
    {
        bool is_delmark = datafile_path.ends_with(re2::StringPiece(details::DELMARK_SUFFIX.data(), details::DELMARK_SUFFIX.size()));
        if (is_delmark)
        {
            datafile_path.remove_suffix(details::DELMARK_SUFFIX.size());
            res.type = S3FilenameType::DelMark;
        }
        else
        {
            if (datafile_path.starts_with("dat_"))
            {
                // "dat_${upload_seq}_${idx}"
                res.type = S3FilenameType::DataFile;
            }
            else if (datafile_path.starts_with("t_") || datafile_path.starts_with("ks_"))
            {
                // "t_${table_id}/dmf_${id}"
                // "ks_${table_id}/dmf_${id}"
                res.type = S3FilenameType::DataFile;
            }
            else
            {
                res.type = S3FilenameType::Invalid;
            }
        }
    }
    res.data_subpath = std::string_view(datafile_path.data(), datafile_path.size());
    return res;
}

S3FilenameView S3FilenameView::fromStoreKeyPrefix(const std::string_view prefix)
{
    S3FilenameView res{.type = S3FilenameType::Invalid};
    re2::StringPiece prefix_sp{prefix.data(), prefix.size()};
    if (!re2::RE2::FullMatch(prefix_sp, details::rgx_store_prefix, &res.store_id))
        return res;

    res.type = S3FilenameType::StorePrefix;
    return res;
}

//==== Data file utils ====//

String S3FilenameView::getLockPrefix() const
{
    RUNTIME_CHECK(isDataFile());
    return fmt::format("lock/s{}/{}.lock_", store_id, data_subpath);
}

String S3FilenameView::getLockKey(StoreID lock_store_id, UInt64 lock_seq) const
{
    RUNTIME_CHECK(isDataFile());
    return fmt::format("lock/s{}/{}.lock_s{}_{}", store_id, data_subpath, lock_store_id, lock_seq);
}

String S3FilenameView::getDelMarkKey() const
{
    switch (type)
    {
    case S3FilenameType::DataFile:
        return fmt::format("s{}/data/{}{}", store_id, data_subpath, details::DELMARK_SUFFIX);
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

UInt64 S3FilenameView::getUploadSequence() const
{
    UInt64 upload_seq = 0;
    switch (type)
    {
    case S3FilenameType::CheckpointManifest:
    {
        re2::StringPiece path_sp{data_subpath.data(), data_subpath.size()};
        if (!re2::RE2::FullMatch(path_sp, details::rgx_subpath_manifest, &upload_seq))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid {}, path={}", magic_enum::enum_name(type), data_subpath);
        return upload_seq;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

//==== Lock file utils ====//

S3FilenameView S3FilenameView::asDataFile() const
{
    switch (type)
    {
    case S3FilenameType::LockFile:
    case S3FilenameType::DelMark:
        return S3FilenameView{.type = S3FilenameType::DataFile, .store_id = store_id, .data_subpath = data_subpath};
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

S3FilenameView::LockInfo S3FilenameView::getLockInfo() const
{
    LockInfo lock_info;
    switch (type)
    {
    case S3FilenameType::LockFile:
    {
        RUNTIME_CHECK(!lock_suffix.empty());
        re2::StringPiece lock_suffix_sp{lock_suffix.data(), lock_suffix.size()};
        if (!re2::RE2::FullMatch(lock_suffix_sp, details::rgx_lock_suffix, &lock_info.store_id, &lock_info.sequence))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid {}, lock_suffix={}", magic_enum::enum_name(type), lock_suffix);
        return lock_info;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

//==== Generate S3 key from raw parts ====//

S3Filename S3Filename::fromStoreId(StoreID store_id)
{
    return S3Filename{
        .type = S3FilenameType::StorePrefix,
        .store_id = store_id,
    };
}

S3Filename S3Filename::fromDMFileOID(const DMFileOID & oid)
{
    return S3Filename{
        .type = S3FilenameType::DataFile,
        .store_id = oid.store_id,
        .data_subpath = fmt::format("t_{}/dmf_{}", oid.table_id, oid.file_id),
    };
}

S3Filename S3Filename::newCheckpointData(StoreID store_id, UInt64 upload_seq, UInt64 file_idx)
{
    return S3Filename{
        .type = S3FilenameType::DataFile,
        .store_id = store_id,
        .data_subpath = fmt::format("dat_{}_{}", upload_seq, file_idx),
    };
}

S3Filename S3Filename::newCheckpointManifest(StoreID store_id, UInt64 upload_seq)
{
    return S3Filename{
        .type = S3FilenameType::CheckpointManifest,
        .store_id = store_id,
        .data_subpath = fmt::format("mf_{}", upload_seq),
    };
}

} // namespace DB::S3
