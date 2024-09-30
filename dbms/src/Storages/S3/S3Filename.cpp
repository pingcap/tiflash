// Copyright 2023 PingCAP, Inc.
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
#include <Common/StringUtils/StringUtils.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Types.h>
#include <Storages/S3/S3Filename.h>
#include <common/types.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <magic_enum.hpp>
#include <string_view>

namespace DB::S3
{
//==== Serialize/Deserialize ====//

namespace details
{
// Ref: https://github.com/google/re2/wiki/Syntax

/// parsing LockFile
const static re2::RE2 rgx_lock("^lock/s(?P<store_id>[0-9]+)/(?P<data_subpath>.+)$");
const static re2::RE2 rgx_lock_suffix(".lock_s(?P<lock_store_id>[0-9]+)_(?P<lock_seq>[0-9]+)");

/// parsing CheckpointManifest or DataFile
const static re2::RE2 rgx_store_prefix("^s(?P<store_id>[0-9]+)/$");
const static re2::RE2 rgx_data_or_manifest("^s(?P<store_id>[0-9]+)/(data|manifest)/(?P<data_subpath>.+)$");
const static re2::RE2 rgx_subpath_manifest("mf_(?P<upload_seq>[0-9]+)");

/// parsing DTFile
const static re2::RE2 rgx_subpath_dtfile("t_(?P<table_id>[0-9]+)/dmf_(?P<file_id>[0-9]+)");
const static re2::RE2 rgx_subpath_keyspace_dtfile(
    "ks_(?P<keyspace_id>[0-9]+)_t_(?P<table_id>[0-9]+)/dmf_(?P<file_id>[0-9]+)");

constexpr static std::string_view DELMARK_SUFFIX = ".del";

// clang-format off

constexpr static std::string_view fmt_allstore_prefix  = "s";
constexpr static std::string_view fmt_store_prefix     = "s{store_id}/";
constexpr static std::string_view fmt_manifest_prefix  = "s{store_id}/manifest/";
constexpr static std::string_view fmt_manifest         = "s{store_id}/manifest/{subpath}";
constexpr static std::string_view fmt_subpath_manifest                       = "mf_{seq}";

constexpr static std::string_view fmt_datafile_prefix  = "s{store_id}/data/";
constexpr static std::string_view fmt_data_file        = "s{store_id}/data/{subpath}";
constexpr static std::string_view fmt_subpath_checkpoint_data            = "dat_{seq}_{index}";
constexpr static std::string_view fmt_subpath_dttable                    = "t_{table_id}";
constexpr static std::string_view fmt_subpath_keyspace_dttable           = "ks_{keyspace_id}_t_{table_id}";
constexpr static std::string_view fmt_subpath_dtfile                     = "t_{table_id}/dmf_{id}";
constexpr static std::string_view fmt_subpath_keyspace_dtfile            = "ks_{keyspace_id}_t_{table_id}/dmf_{id}";

// lock prefix for all stores
constexpr static std::string_view fmt_lock_prefix = "lock/";
// lock prefix for a specific data file
constexpr static std::string_view fmt_lock_datafile_prefix = "lock/s{store_id}/{subpath}.lock_";
constexpr static std::string_view fmt_lock_file = "lock/s{store_id}/{subpath}.lock_s{lock_store}_{lock_seq}";

// If you want to read/write S3 object as file throught `FileProvider`, file path must starts with `s3_filename_prefix`.
constexpr static std::string_view s3_filename_prefix = "s3://";
// clang-format on


String toFullKey(const S3FilenameType type, const StoreID store_id, const std::string_view data_subpath)
{
    switch (type)
    {
    case S3FilenameType::DataFile:
        return fmt::format(fmt_data_file, fmt::arg("store_id", store_id), fmt::arg("subpath", data_subpath));
    case S3FilenameType::CheckpointManifest:
        return fmt::format(fmt_manifest, fmt::arg("store_id", store_id), fmt::arg("subpath", data_subpath));
    case S3FilenameType::StorePrefix:
        return fmt::format(fmt_store_prefix, fmt::arg("store_id", store_id));
    default:
        throw Exception(fmt::format("Not support type! type={}", magic_enum::enum_name(type)));
    }
    __builtin_unreachable();
}

} // namespace details

bool S3FilenameView::isDMFile() const
{
    // dmfile with table prefix
    static_assert(details::fmt_subpath_dtfile[0] == 't', "dtfile prefix changed!");
    static_assert(details::fmt_subpath_dtfile[1] == '_', "dtfile prefix changed!");

    // dmfile with keyspace prefix
    static_assert(details::fmt_subpath_keyspace_dtfile[0] == 'k', "keyspace dtfile prefix changed!");
    static_assert(details::fmt_subpath_keyspace_dtfile[1] == 's', "keyspace dtfile prefix changed!");
    static_assert(details::fmt_subpath_keyspace_dtfile[2] == '_', "keyspace dtfile prefix changed!");

    return (startsWith(data_subpath, "t_") || startsWith(data_subpath, "ks_"));
}

DMFileOID S3FilenameView::getDMFileOID() const
{
    RUNTIME_CHECK(isDMFile());
    TableID table_id;
    UInt64 file_id;
    re2::StringPiece prefix_sp{data_subpath.data(), data_subpath.size()};
    if (startsWith(data_subpath, "t_"))
    {
        RUNTIME_CHECK(re2::RE2::FullMatch(prefix_sp, details::rgx_subpath_dtfile, &table_id, &file_id));
        return DMFileOID{
            .store_id = store_id,
            .keyspace_id = NullspaceID,
            .table_id = table_id,
            .file_id = file_id,
        };
    }
    else
    {
        KeyspaceID keyspace_id;
        RUNTIME_CHECK(
            re2::RE2::FullMatch(prefix_sp, details::rgx_subpath_keyspace_dtfile, &keyspace_id, &table_id, &file_id));
        return DMFileOID{
            .store_id = store_id,
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .file_id = file_id,
        };
    }
}

String S3FilenameView::toFullKey() const
{
    return details::toFullKey(type, store_id, data_subpath);
}

String S3Filename::toFullKey() const
{
    return details::toFullKey(type, store_id, data_subpath);
}

String S3Filename::toFullKeyWithPrefix() const
{
    return fmt::format("{}{}", details::s3_filename_prefix, toFullKey());
}

String S3Filename::toManifestPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return fmt::format(details::fmt_manifest_prefix, fmt::arg("store_id", store_id));
}

String S3Filename::toDataPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return fmt::format(details::fmt_datafile_prefix, fmt::arg("store_id", store_id));
}

String S3Filename::getLockPrefix()
{
    return String(details::fmt_lock_prefix);
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
        res.lock_suffix
            = std::string_view(datafile_path.begin() + lock_start_npos, datafile_path.size() - lock_start_npos);
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
        bool is_delmark
            = datafile_path.ends_with(re2::StringPiece(details::DELMARK_SUFFIX.data(), details::DELMARK_SUFFIX.size()));
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

S3FilenameView S3FilenameView::fromKeyWithPrefix(std::string_view fullpath)
{
    if (startsWith(fullpath, details::s3_filename_prefix))
    {
        return fromKey(fullpath.substr(details::s3_filename_prefix.size()));
    }
    return S3FilenameView{}; // Invalid
}

//==== Data file utils ====//

String S3FilenameView::getLockPrefix() const
{
    RUNTIME_CHECK(isDataFile());
    return fmt::format(
        details::fmt_lock_datafile_prefix,
        fmt::arg("store_id", store_id),
        fmt::arg("subpath", data_subpath));
}

String S3FilenameView::getLockKey(StoreID lock_store_id, UInt64 lock_seq) const
{
    RUNTIME_CHECK(isDataFile());
    return fmt::format(
        details::fmt_lock_file,
        fmt::arg("store_id", store_id),
        fmt::arg("subpath", data_subpath),
        fmt::arg("lock_store", lock_store_id),
        fmt::arg("lock_seq", lock_seq));
}

String S3FilenameView::getDelMarkKey() const
{
    switch (type)
    {
    case S3FilenameType::DataFile:
        return fmt::format(details::fmt_data_file, fmt::arg("store_id", store_id), fmt::arg("subpath", data_subpath))
            + String(details::DELMARK_SUFFIX);
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid {}, path={}",
                magic_enum::enum_name(type),
                data_subpath);
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid {}, lock_suffix={}",
                magic_enum::enum_name(type),
                lock_suffix);
        return lock_info;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

//==== Generate S3 key from raw parts ====//

String S3Filename::allStorePrefix()
{
    return String(details::fmt_allstore_prefix);
}

S3Filename S3Filename::fromStoreId(StoreID store_id)
{
    return S3Filename{
        .type = S3FilenameType::StorePrefix,
        .store_id = store_id,
    };
}

S3Filename S3Filename::fromDMFileOID(const DMFileOID & oid)
{
    if (oid.keyspace_id == NullspaceID)
    {
        return S3Filename{
            .type = S3FilenameType::DataFile,
            .store_id = oid.store_id,
            .data_subpath
            = fmt::format(details::fmt_subpath_dtfile, fmt::arg("table_id", oid.table_id), fmt::arg("id", oid.file_id)),
        };
    }
    else
    {
        return S3Filename{
            .type = S3FilenameType::DataFile,
            .store_id = oid.store_id,
            .data_subpath = fmt::format(
                details::fmt_subpath_keyspace_dtfile,
                fmt::arg("table_id", oid.table_id),
                fmt::arg("keyspace_id", oid.keyspace_id),
                fmt::arg("id", oid.file_id)),
        };
    }
}

S3Filename S3Filename::fromTableID(StoreID store_id, KeyspaceID keyspace_id, TableID table_id)
{
    if (keyspace_id == NullspaceID)
    {
        return S3Filename{
            .type = S3FilenameType::DataFile,
            .store_id = store_id,
            .data_subpath = fmt::format(details::fmt_subpath_dttable, fmt::arg("table_id", table_id)),
        };
    }
    else
    {
        return S3Filename{
            .type = S3FilenameType::DataFile,
            .store_id = store_id,
            .data_subpath = fmt::format(
                details::fmt_subpath_keyspace_dttable,
                fmt::arg("keyspace_id", keyspace_id),
                fmt::arg("table_id", table_id)),
        };
    }
}

S3Filename S3Filename::newCheckpointData(StoreID store_id, UInt64 upload_seq, UInt64 file_idx)
{
    return S3Filename{
        .type = S3FilenameType::DataFile,
        .store_id = store_id,
        .data_subpath
        = fmt::format(details::fmt_subpath_checkpoint_data, fmt::arg("seq", upload_seq), fmt::arg("index", file_idx)),
    };
}

String S3Filename::newCheckpointDataNameTemplate(StoreID store_id, UInt64 lock_seq)
{
    return fmt::format(
        details::fmt_lock_file,
        fmt::arg("store_id", store_id),
        fmt::arg("subpath", details::fmt_subpath_checkpoint_data), // available placeholder `seq`, `index`
        fmt::arg("lock_store", store_id),
        fmt::arg("lock_seq", lock_seq));
}

String S3Filename::newCheckpointManifestNameTemplate(StoreID store_id)
{
    return fmt::format(
        details::fmt_manifest,
        fmt::arg("store_id", store_id),
        fmt::arg("subpath", details::fmt_subpath_manifest) // available placeholder `seq`
    );
}

S3Filename S3Filename::newCheckpointManifest(StoreID store_id, UInt64 upload_seq)
{
    return S3Filename{
        .type = S3FilenameType::CheckpointManifest,
        .store_id = store_id,
        .data_subpath = fmt::format(details::fmt_subpath_manifest, fmt::arg("seq", upload_seq)),
    };
}

} // namespace DB::S3
