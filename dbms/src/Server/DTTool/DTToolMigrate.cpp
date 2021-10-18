#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/ChecksumBuffer.h>
#include <IO/IOSWrapper.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <fcntl.h>

#include <boost/program_options.hpp>
#include <iostream>

namespace DTTool::Migrate
{
bool isIgnoredInMigration(const DB::DM::DMFile & file, std::string & target)
{
    UNUSED(file);
    return target == "NGC"; // this is not exported
}
bool needFrameMigration(const DB::DM::DMFile & file, std::string & target)
{
    return endsWith(target, ".mrk")
        || endsWith(target, ".dat")
        || endsWith(target, ".idx")
        || file.packStatFileName() == target;
}
bool isRecognizable(const DB::DM::DMFile & file, std::string & target)
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
    "  --version     Target dmfile version. [default: 2] [available: 1, 2]\n"
    "  --algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  --frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  --file-id     Target file id.\n"
    "  --workdir     Target directory.\n"
    "  --nokeep      Do not keep old version.\n"
    "  --dry         Dry run: only print change list.";

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


int migrateServiceMain(DB::Context & context, const MigrateArgs & args)
{
    DirLock _lock{args.workdir};
    auto migrate_path = args.workdir + "/" + "dmf_" + DB::toString(args.file_id) + ".migrate/";
    auto fp = context.getFileProvider();
    auto src_file = DB::DM::DMFile::restore(fp, args.file_id, 0, args.workdir, DB::DM::DMFile::ReadMetaMode::all());
    std::cout << "source version: " << (src_file->getConfiguration() ? 2 : 1) << std::endl;
    std::cout << "source bytes: " << (src_file->getBytesOnDisk()) << std::endl;
    std::cout << "creating migration temporary directory" << std::endl;
    if (!args.dry_mode && !Poco::File(migrate_path).createDirectory())
    {
        std::cerr << "could not create: " << migrate_path << std::endl;
        return 1;
    }
    DB::DM::DMConfigurationOpt option{};
    if (args.version == 2)
    {
        DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V3;
        option.emplace(std::map<std::string, std::string>{}, args.frame, args.algorithm);
    }
    else
    {
        DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V2;
    }
    auto & embedded_checksums = option->getEmbeddedChecksum();
    // meta.txt
    {
        auto source_path = src_file->metaPath();
        auto target_path = migrate_path + src_file->metaFileName();
        std::cout << "migrating " << src_file->metaFileName() << " from " << source_path << " to " << target_path << std::endl;
        auto file = Poco::File(source_path);
        if (!args.dry_mode && file.exists())
        {
            auto meta_size = Poco::File(source_path).getSize();
            auto file_buf = DB::DM::openForRead(fp, source_path, src_file->encryptionMetaPath(), meta_size);
            auto meta_buf = std::vector<char>(meta_size);

            file_buf.readBig(meta_buf.data(), meta_size);

            // override first format line:
            {
                auto buffer = DB::WriteBufferFromVector(meta_buf);
                writeString("DTFile format: ", buffer);
                writeIntText(option ? DB::DMFileFormat::V2 : DB::DMFileFormat::V1, buffer);
                writeString("\n", buffer);
            }

            if (option)
            {
                auto digest = option->createUnifiedDigest();
                digest->update(meta_buf.data(), meta_size);
                embedded_checksums[src_file->metaFileName()] = digest->raw();
            }

            DB::WriteBufferFromFileProvider buf(fp, target_path, src_file->encryptionMetaPath(), false, nullptr, 4096);
            buf.write(meta_buf.data(), meta_size);
            buf.sync();
        }
    }
    // pack property
    {
        auto source_path = src_file->packPropertyPath();
        auto target_path = migrate_path + src_file->packPropertyFileName();
        std::cout << "migrating " << src_file->packPropertyFileName() << " from " << source_path << " to " << target_path << std::endl;
        auto file = Poco::File(source_path);
        if (!args.dry_mode && file.exists())
        {
            file.copyTo(target_path);
            if (option)
            {
                auto digest = option->createUnifiedDigest();
                auto property = src_file->pack_properties.SerializeAsString();
                digest->update(property.data(), property.size());
                embedded_checksums[src_file->packPropertyFileName()] = digest->raw();
            }
        }
    }

    auto prefix = args.workdir + "/dmf_" + DB::toString(args.file_id);

    std::vector<std::string> sub;
    {
        auto file = Poco::File{prefix};
        file.list(sub);
    }
    for (auto & i : sub)
    {
        if (!isRecognizable(*src_file, i))
        {
            std::cerr << "target file: " << i << " is not recognizable by this tool" << std::endl;
        }
        if (endsWith(i, ".mrk") || endsWith(i, ".dat") || endsWith(i, ".idx") || i == "pack")
        {
            auto source_path = src_file->path() + "/" + i;
            auto target_path = migrate_path + i;
            std::cout << "migrating " << i << " from " << source_path << " to " << target_path << std::endl;

            if (!args.dry_mode)
            {
                DB::ReadBufferPtr read_buffer;
                if (src_file->getConfiguration())
                {
                    read_buffer = DB::createReadBufferFromFileBaseByFileProvider(
                        fp,
                        source_path,
                        DB::EncryptionPath(source_path, i),
                        src_file->getConfiguration()->getChecksumFrameLength(),
                        nullptr,
                        src_file->getConfiguration()->getChecksumAlgorithm(),
                        src_file->getConfiguration()->getChecksumFrameLength());
                }
                else
                {
                    read_buffer = DB::createReadBufferFromFileBaseByFileProvider(
                        fp,
                        source_path,
                        DB::EncryptionPath(source_path, i),
                        DBMS_DEFAULT_BUFFER_SIZE,
                        0,
                        nullptr);
                }
                DB::WriteBufferPtr write_buffer;
                size_t buffer_size;
                if (option)
                {
                    write_buffer = DB::createWriteBufferFromFileBaseByFileProvider(
                        fp,
                        target_path,
                        DB::EncryptionPath(source_path, i), // use original path
                        false,
                        nullptr,
                        option->getChecksumAlgorithm(),
                        option->getChecksumFrameLength());
                    buffer_size = option->getChecksumFrameLength();
                }
                else
                {
                    write_buffer = DB::createWriteBufferFromFileBaseByFileProvider(
                        fp,
                        target_path,
                        DB::EncryptionPath(source_path, i), // use original path
                        false,
                        nullptr,
                        DBMS_DEFAULT_BUFFER_SIZE,
                        0,
                        DBMS_DEFAULT_BUFFER_SIZE);
                    buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
                }
                while (auto size = read_buffer->readBig(write_buffer->buffer().begin(), buffer_size))
                {
                    write_buffer->buffer().resize(size);
                    write_buffer->position() = write_buffer->buffer().begin() + size;
                    write_buffer->next();
                }
            }
        }
    }

    if (option)
    {
        auto target_path = migrate_path + "config";
        std::cout << "writing new config to " << target_path << std::endl;
        if (!args.dry_mode)
        {
            DB::WriteBufferFromFileProvider buf(fp, target_path, src_file->encryptionMetaPath(), false, nullptr, 4096);
            DB::OutputStreamWrapper wrapper(buf);
            wrapper << *option;
            buf.sync();
        }
    }

    // rename old dmfile
    auto original_path = src_file->path();
    auto backup_path = src_file->path() + ".old";
    {
        std::cout << "renaming old dmfile to " << backup_path << std::endl;
        if (!args.dry_mode)
        {
            auto file = Poco::File(original_path);
            file.renameTo(backup_path);
        }
    }
    // rename new dmfile
    {
        std::cout << "renaming new dmfile to " << original_path << std::endl;
        if (!args.dry_mode)
        {
            auto file = Poco::File(migrate_path);
            file.renameTo(original_path);
        }
    }

    if (args.no_keep)
    {
        std::cout << "removing " << backup_path << std::endl;
        if (!args.dry_mode)
        {
            auto file = Poco::File(backup_path);
            file.remove(true);
        }
    }

    std::cout << "checking meta status for " << original_path << std::endl;
    if (!args.dry_mode)
    {
        DB::DM::DMFile::restore(fp, args.file_id, 1, args.workdir, DB::DM::DMFile::ReadMetaMode::all());
    }

    std::cout << "migration finished" << std::endl;

    return 0;
}

int migrateEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge Migration"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool dry_mode = false;
    bool no_keep = false;
    // clang-format off
    options.add_options()
        ("help", "")
        ("version", bpo::value<size_t>()->default_value(2))
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("workdir", bpo::value<std::string>()->required())
        ("config-file", bpo::value<std::string>()->required())
        ("file-id", bpo::value<size_t>()->required())
        ("dry", bpo::bool_switch(&dry_mode))
        ("nokeep", bpo::bool_switch(&no_keep));
    // clang-format on

    bpo::store(bpo::command_line_parser(opts)
                   .options(options)
                   .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                   .run(),
               vm);

    try
    {
        if (vm.count("help"))
        {
            std::cout << MIGRATE_HELP << std::endl;
            return 0;
        }
        bpo::notify(vm);
        MigrateArgs args{};
        args.version = vm["version"].as<size_t>();
        if (args.version < 1 || args.version > 2)
        {
            std::cerr << "invalid dmfile version: " << args.version << std::endl;
            return -EINVAL;
        }
        args.no_keep = no_keep;
        args.dry_mode = dry_mode;
        args.workdir = vm["workdir"].as<std::string>();
        args.file_id = vm["file-id"].as<size_t>();
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

        CLIService service(migrateServiceMain, args, vm["config-file"].as<std::string>());
        return service.run({""});
    }
    catch (const boost::wrapexcept<boost::program_options::required_option> & exception)
    {
        std::cerr << exception.what() << std::endl
                  << MIGRATE_HELP << std::endl;
        return 1;
    }
}
} // namespace DTTool::Migrate