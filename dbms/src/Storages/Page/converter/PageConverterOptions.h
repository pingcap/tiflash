#pragma once

#include <Storages/Page/PageDefines.h>
#include <fmt/format.h>

#include <boost/program_options.hpp>
#include <string>

struct PageConverterOptions
{
    bool keep_old_data = true;
    bool old_data_packed = true;
    bool use_same_path = true;

    UInt64 max_blob_file_size = DB::BLOBFILE_LIMIT_SIZE;

    inline static String packed_manifest_file_path = "PackedPagesV2.manifest";
    inline static String packed_name = "PackedPagesV2";

    std::string packed_path = "PackedPagesV2/";
    std::string log_save_path = "./";

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

    static PageConverterOptions parse(int argc, char ** argv)
    {
        namespace po = boost::program_options;
        using po::value;
        po::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message") //
            ("keep_old_data,K", value<bool>()->default_value(true), "Keep the old version of pages.") //
            ("old_data_packed,O", value<bool>()->default_value(true), "Packed the old pages. If the converter cannot successfully convert the pages into V3 version, you can use the restore function to restore the old data") //
            ("use_same_path,U", value<bool>()->default_value(true), "Use the same path.") //
            ("max_blob_file_size,U", value<UInt64>()->default_value(DB::BLOBFILE_LIMIT_SIZE), "Max size of single BlobFile") //
            ("packed_path,C", value<std::string>()->default_value("./PackedPagesV2/"), "Old pages packed path. It only takes effect when the `old_data_packed` is true.") //
            ("log_save_path,L", value<std::string>()->default_value("./"), "TiFlash log saved path.") //
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

        PageConverterOptions converter_opts;
        converter_opts.keep_old_data = options["keep_old_data"].as<bool>();
        converter_opts.old_data_packed = options["old_data_packed"].as<bool>();
        converter_opts.use_same_path = options["use_same_path"].as<bool>();
        converter_opts.max_blob_file_size = options["max_blob_file_size"].as<UInt64>();

        if (options.count("paths"))
        {
            converter_opts.paths = options["paths"].as<std::vector<std::string>>();
        }
        else
        {
            std::cerr << "Invalid arg: `paths`." << std::endl;
            std::cerr << desc << std::endl;
            exit(0);
        }


        if (!converter_opts.use_same_path)
        {
            if (options.count("store_paths"))
            {
                converter_opts.store_paths = options["store_paths"].as<std::vector<std::string>>();
            }
            else
            {
                std::cerr << "Invalid arg: `store_paths`. If `use_same_path` is false, `store_paths` must be set." << std::endl;
                std::cerr << desc << std::endl;
                exit(0);
            }
        }

        if (converter_opts.old_data_packed)
        {
            converter_opts.packed_path = options["packed_path"].as<std::string>();
        }

        if (options.count("log_save_path"))
        {
            converter_opts.log_save_path = options["log_save_path"].as<std::string>();
        }

        return converter_opts;
    }
};