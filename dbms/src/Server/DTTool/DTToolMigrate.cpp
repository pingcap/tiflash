#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <IO/ChecksumBuffer.h>
#include <IO/IOSWrapper.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <fcntl.h>

#include <boost/program_options.hpp>
#include <iostream>

namespace DTTool::Migrate
{
bool isIgnoredInMigration(const DB::DM::DMFile & file, const std::string & target)
{
    UNUSED(file);
    return target == "NGC"; // this is not exported
}
bool needFrameMigration(const DB::DM::DMFile & file, const std::string & target)
{
    return endsWith(target, ".mrk")
        || endsWith(target, ".dat")
        || endsWith(target, ".idx")
        || file.packStatFileName() == target;
}
bool isRecognizable(const DB::DM::DMFile & file, const std::string & target)
{
    return file.metaFileName() == target
        || file.configurationFileName() == target
        || file.packPropertyFileName() == target
        || needFrameMigration(file, target)
        || isIgnoredInMigration(file, target);
}

namespace bpo = boost::program_options;

// clang-format off
static constexpr char MIGRATE_HELP[] =
    "Usage: migrate [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --version     Target dtfile version. [default: 2] [available: 1, 2]\n"
    "  --algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  --frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  --compression Compression method. [default: lz4] [available: lz4, lz4hc, zstd, none]\n"
    "  --level       Compression level. [default: -1 (auto)]\n"
    "  --file-id     Target file id.\n"
    "  --workdir     Target directory.\n"
    "  --nokeep      Do not keep old version.\n"
    "  --dry         Dry run: only print change list.\n"
    "  --imitative   Use imitative context instead. (encryption is not supported in this mode)\n"
    "  --config-file Path to TiFlash config (tiflash.toml).";

// clang-format on

struct DirLock
{
    int dir{};
    struct flock lock
    {
    };
    std::string workdir_lock;

    explicit DirLock(const std::string & workdir_)
        : workdir_lock(workdir_ + "/LOCK")
    {
        dir = ::open(workdir_lock.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP);
        if (dir == -1)
        {
            throw DB::ErrnoException(fmt::format("cannot open target for lock: {}", workdir_lock), 0, errno);
        }
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        auto result = ::fcntl(dir, F_SETLK, &lock);
        if (result == -1)
        {
            throw DB::ErrnoException(fmt::format("cannot lock target: {}", workdir_lock), result, errno);
        }
    }

    ~DirLock()
    {
        lock.l_type = F_UNLCK;
        auto result = ::fcntl(dir, F_SETLK, &lock);
        if (result != 0)
        {
            std::cerr << fmt::format("cannot unlock target: {}, errno: {}, msg: {}", workdir_lock, errno, strerror(errno)) << std::endl;
        }
        if (::close(dir) != 0)
        {
            std::cerr << fmt::format("cannot close target: {}, errno: {}, msg: {}", workdir_lock, errno, strerror(errno)) << std::endl;
        }
        if (Poco::File file(workdir_lock); file.exists())
        {
            file.remove(true);
        }
    }
};

struct MigrationHouseKeeper
{
    bool success;
    bool no_keep;
    Poco::File migration_temp_dir;
    Poco::File migration_target_dir;
    size_t migration_file;

    DB::StorageFormatVersion old_version;
    MigrationHouseKeeper(std::string migration_temp_dir,
                         std::string migration_target_dir,
                         size_t migration_file,
                         bool no_keep)
        : success(false)
        , no_keep(no_keep)
        , migration_temp_dir(migration_temp_dir)
        , migration_target_dir(migration_target_dir)
        , migration_file(migration_file)
        , old_version(DB::STORAGE_FORMAT_CURRENT)
    {
        if (!this->migration_temp_dir.createDirectory())
        {
            throw DB::Exception("cannot create migration temp dir " + migration_temp_dir);
        }
    }

    void markSuccess()
    {
        success = true;
    }

    void setStorageVersion(DB::StorageFormatVersion version)
    {
        old_version = DB::STORAGE_FORMAT_CURRENT;
        DB::STORAGE_FORMAT_CURRENT = version;
    }

    ~MigrationHouseKeeper() noexcept(false)
    {
        DB::STORAGE_FORMAT_CURRENT = old_version;
        if (success)
        {
            auto target_path = fmt::format("{}/dmf_{}", migration_target_dir.path(), migration_file);
            Poco::File old{target_path};
            if (no_keep)
            {
                old.remove(true);
            }
            else
            {
                old.moveTo(fmt::format("{}.old", target_path));
            }
            Poco::File target{fmt::format("{}/dmf_{}", migration_temp_dir.path(), migration_file)};
            target.copyTo(target_path);
        }
        migration_temp_dir.remove(true);
    }
};


int migrateServiceMain(DB::Context & context, const MigrateArgs & args)
{
    // exclude other dttool from running on the same directory
    DirLock lock{args.workdir};
    // from this part, the base daemon is running, so we use logger instead
    auto * logger = &Poco::Logger::get("DTToolMigration");
    {
        // the HouseKeeper is to make sure the directories will be removed or renamed
        // after the running.
        MigrationHouseKeeper keeper{
            fmt::format("{}/.migration", args.workdir),
            args.workdir,
            args.file_id,
            args.no_keep};
        auto src_file = DB::DM::DMFile::restore(context.getFileProvider(), args.file_id, 0, args.workdir, DB::DM::DMFile::ReadMetaMode::all());
        LOG_FMT_INFO(logger, "source version: {}", (src_file->getConfiguration() ? 2 : 1));
        LOG_FMT_INFO(logger, "source bytes: {}", src_file->getBytesOnDisk());
        LOG_FMT_INFO(logger, "migration temporary directory: {}", keeper.migration_temp_dir.path().c_str());
        DB::DM::DMConfigurationOpt option{};

        // if new format is the target, we construct a config file.
        switch (args.version)
        {
        case DB::DMFileFormat::V2:
            keeper.setStorageVersion(DB::STORAGE_FORMAT_V3);
            option.emplace(std::map<std::string, std::string>{}, args.frame, args.algorithm);
            break;
        case DB::DMFileFormat::V1:
            keeper.setStorageVersion(DB::STORAGE_FORMAT_V2);
            break;
        default:
            throw DB::Exception(fmt::format("invalid dtfile version: {}", args.version));
        }

        LOG_FMT_INFO(logger, "creating new dtfile");
        auto new_file = DB::DM::DMFile::create(args.file_id, keeper.migration_temp_dir.path(), false, std::move(option));

        LOG_FMT_INFO(logger, "creating input stream");
        auto input_stream = DB::DM::createSimpleBlockInputStream(context, src_file);

        LOG_FMT_INFO(logger, "creating output stream");
        auto output_stream = DB::DM::DMFileBlockOutputStream(
            context,
            new_file,
            src_file->getColumnDefines(),
            {},
            {args.compression_method, args.compression_level});

        input_stream->readPrefix();
        if (!args.dry_mode)
            output_stream.writePrefix();
        auto stat_iter = src_file->pack_stats.begin();
        auto properties_iter = src_file->pack_properties.property().begin();
        size_t counter = 0;
        // iterate all blocks and rewrite them to new dtfile
        while (auto block = input_stream->read())
        {
            LOG_FMT_INFO(logger, "migrating block {} ( size: {} )", counter++, block.bytes());
            if (!args.dry_mode)
                output_stream.write(
                    block,
                    {stat_iter->not_clean, properties_iter->num_rows(), properties_iter->gc_hint_version()});
            stat_iter++;
            properties_iter++;
        }
        input_stream->readSuffix();
        if (!args.dry_mode)
        {
            output_stream.writeSuffix();
            keeper.markSuccess();
        }

        LOG_FMT_INFO(logger, "checking meta status for new file");
        if (!args.dry_mode)
        {
            DB::DM::DMFile::restore(context.getFileProvider(), args.file_id, 1, keeper.migration_temp_dir.path(), DB::DM::DMFile::ReadMetaMode::all());
        }
    }
    LOG_FMT_INFO(logger, "migration finished");

    return 0;
}

int migrateEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function)
{
    bpo::options_description options{"Delta Merge Migration"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool dry_mode = false;
    bool no_keep = false;
    bool imitative = false;
    // clang-format off
    options.add_options()
        ("help", "")
        ("version", bpo::value<size_t>()->default_value(2))
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("workdir", bpo::value<std::string>()->required())
        ("config-file", bpo::value<std::string>())
        ("file-id", bpo::value<size_t>()->required())
        ("dry", bpo::bool_switch(&dry_mode))
        ("compression", bpo::value<std::string>()->default_value("lz4"))
        ("level", bpo::value<int>()->default_value(-1))
        ("imitative", bpo::bool_switch(&imitative))
        ("nokeep", bpo::bool_switch(&no_keep));
    // clang-format on

    bpo::store(bpo::command_line_parser(opts)
                   .options(options)
                   .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                   .run(),
               vm);

    try
    {
        if (vm.count("help") != 0)
        {
            std::cout << MIGRATE_HELP << std::endl;
            return 0;
        }

        bpo::notify(vm);

        if (imitative && vm.count("config-file") != 0)
        {
            std::cerr << "config-file is not allowed in imitative mode" << std::endl;
            return -EINVAL;
        }
        else if (!imitative && vm.count("config-file") == 0)
        {
            std::cerr << "config-file is required in proxy mode" << std::endl;
            return -EINVAL;
        }

        MigrateArgs args{};
        args.version = vm["version"].as<size_t>();
        if (args.version < 1 || args.version > 2)
        {
            std::cerr << "invalid dtfile version: " << args.version << std::endl;
            return -EINVAL;
        }
        args.no_keep = no_keep;
        args.dry_mode = dry_mode;
        args.workdir = vm["workdir"].as<std::string>();
        args.file_id = vm["file-id"].as<size_t>();

        {
            auto compression_method = vm["compression"].as<std::string>();
            if (compression_method == "lz4")
            {
                args.compression_method = DB::CompressionMethod::LZ4;
            }
            else if (compression_method == "lz4hc")
            {
                args.compression_method = DB::CompressionMethod::LZ4HC;
            }
            else if (compression_method == "zstd")
            {
                args.compression_method = DB::CompressionMethod::ZSTD;
            }
            else if (compression_method == "none")
            {
                args.compression_method = DB::CompressionMethod::NONE;
            }
            else
            {
                std::cerr << "invalid compression method: " << compression_method << std::endl;
                return -EINVAL;
            }

            auto compression_level = vm["level"].as<int>();
            if (compression_level == -1)
            {
                args.compression_level = DB::CompressionSettings::getDefaultLevel(args.compression_method);
            }
            else
            {
                args.compression_level = compression_level;
            }
        }
        if (args.version == 2)
        {
            args.frame = vm["frame"].as<size_t>();
            auto algorithm_ = vm["algorithm"].as<std::string>();
            if (algorithm_ == "xxh3")
            {
                args.algorithm = DB::ChecksumAlgo::XXH3;
            }
            else if (algorithm_ == "crc32")
            {
                args.algorithm = DB::ChecksumAlgo::CRC32;
            }
            else if (algorithm_ == "crc64")
            {
                args.algorithm = DB::ChecksumAlgo::CRC64;
            }
            else if (algorithm_ == "city128")
            {
                args.algorithm = DB::ChecksumAlgo::City128;
            }
            else if (algorithm_ == "none")
            {
                args.algorithm = DB::ChecksumAlgo::None;
            }
            else
            {
                std::cerr << "invalid algorithm: " << algorithm_ << std::endl;
                return -EINVAL;
            }
        }

        if (imitative)
        {
            auto env = detail::ImitativeEnv{args.workdir};
            return migrateServiceMain(*env.getContext(), args);
        }
        else
        {
            CLIService service(migrateServiceMain, args, vm["config-file"].as<std::string>(), ffi_function);
            return service.run({""});
        }
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl
                  << MIGRATE_HELP << std::endl;
        return 1;
    }
}
} // namespace DTTool::Migrate
