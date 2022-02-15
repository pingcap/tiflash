#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <PageConverter.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <TestUtils/MockDiskDelegator.h>

#include <boost/program_options.hpp>

namespace DB
{
// Define is_background_thread for this binary
// It is required for `RateLimiter` but we do not link with `BackgroundProcessingPool`.
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif

} // namespace DB


struct PageConverterArgs
{
    bool keep_old_data = true;
    bool old_data_packed = true;
    bool use_same_path = true;

    UInt64 max_blob_file_size = DB::BLOBFILE_LIMIT_SIZE;

    std::string packed_path;
    std::string log_save_path;

    std::vector<std::string> paths;
    std::vector<std::string> store_paths;

    String toDebugString() const
    {
        return fmt::format(
            "{{ "
            "keep_old_data: {}, use_same_path: {}."
            "}}",
            keep_old_data,
            use_same_path);
    }

    static PageConverterArgs parse(int argc, char ** argv)
    {
        namespace po = boost::program_options;
        using po::value;
        po::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message") //
            ("keep_old_data,K", value<bool>()->default_value(true), "Keep the old version of pages.") //
            ("old_data_packed,O", value<bool>()->default_value(true), "Packed the old pages. If the converter cannot successfully convert the pages into V3 version, you can use the restore function to restore the old data") //
            ("use_same_path,U", value<bool>()->default_value(true), "Use the same path.") //
            ("max_blob_file_size,U", value<UInt64>()->default_value(DB::BLOBFILE_LIMIT_SIZE), "Max size of single BlobFile") //
            ("packed_path,C", value<std::string>()->default_value("."), "Old pages packed path. It only takes effect when the `old_data_packed` is true.") //
            ("log_save_path,L", value<std::string>()->default_value("."), "TiFlash log saved path.") //
            ("paths,P", value<std::vector<std::string>>(), "Old pages path(s).") //
            ("store_paths,S", value<std::vector<std::string>>(), "Converter data store path(s). It only takes effect when the `use_same_path` is false.");

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);
        po::notify(options);

        if (options.count("help") > 0)
        {
            std::cerr << desc << std::endl;
            exit(0);
        }

        PageConverterArgs args;
        args.keep_old_data = options["keep_old_data"].as<bool>();
        args.old_data_packed = options["old_data_packed"].as<bool>();
        args.use_same_path = options["use_same_path"].as<bool>();
        args.max_blob_file_size = options["max_blob_file_size"].as<UInt64>();

        if (options.count("paths"))
        {
            args.paths = options["paths"].as<std::vector<std::string>>();
        }
        else
        {
            std::cerr << "Invalid arg: `paths`." << std::endl;
            std::cerr << desc << std::endl;
            exit(0);
        }


        if (!args.use_same_path)
        {
            if (options.count("store_paths"))
            {
                args.store_paths = options["store_paths"].as<std::vector<std::string>>();
            }
            else
            {
                std::cerr << "Invalid arg: `store_paths`. If `use_same_path` is false, `store_paths` must be set." << std::endl;
                std::cerr << desc << std::endl;
                exit(0);
            }
        }

        if (args.old_data_packed)
        {
            args.packed_path = options["packed_path"].as<std::string>();
        }

        if (options.count("log_save_path"))
        {
            args.log_save_path = options["log_save_path"].as<std::string>();
        }

        return args;
    }
};

void initGlobalLogger()
{
    // TODO : save log
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new DB::UnifiedLogPatternFormatter);
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");
}

int main(int argc, char ** argv)
try
{
    initGlobalLogger();
    PageConverterArgs args = PageConverterArgs::parse(argc, argv);

    // TBD : Encryption not support
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);
    DB::PSDiskDelegatorPtr delegator;

    if (args.paths.size() == 1)
    {
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(args.paths[0]);
    }
    else
    {
        delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(args.paths);
    }

    DB::PageConverter page_converter(file_provider, delegator);
    page_converter.readFromV2();
    return 0;
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
