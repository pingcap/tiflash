#include <PageConverter.h>
#include <PageConverterOptions.h>
#include <Poco/File.h>
#include <Poco/Path.h>

#include <filesystem>
#include <fstream>
#include <iostream>

bool askComfirm(const std::map<String, String> & manifest_infos)
{
    std::cout << "Begin to restore, the following paths will be overwritten." << std::endl;
    for (const auto & [path, ori_path] : manifest_infos)
    {
        (void)ori_path;
        std::cout << " path : " << ori_path << std::endl;
    }
    std::cout << "Do you want to continue?[y/n]";

    char confirm;
    std::cin >> confirm;
    return confirm == 'y';
}

std::map<String, String> parseManifest(const String & restore_dir_path)
{
    std::map<String, String> manifest_infos;

    // Check manifest exist
    Poco::File restore_manifest(restore_dir_path + "/" + PageConverterOptions::packed_manifest_file_path);
    if (!restore_manifest.exists() || !restore_manifest.isFile())
    {
        std::cerr << restore_manifest.path() << " not existed." << std::endl;
        exit(-1);
    }

    std::ifstream restore_manifest_file(restore_manifest.path());
    if (restore_manifest_file.is_open())
    {
        String line;
        while (std::getline(restore_manifest_file, line))
        {
            auto split_index = line.find(":");

            // Check every line is valid.
            if (split_index == std::string::npos)
            {
                std::cerr << "Read from " << restore_manifest.path() << ". line `" << line << "` is invalid" << std::endl;
                exit(-1);
            }

            // Check sub dir exist.
            Poco::File restore_sub_file(restore_dir_path + "/" + line.substr(split_index + 1, line.length()));
            if (!restore_sub_file.exists())
            {
                std::cerr << "Read from " << restore_manifest.path() << ". `" << restore_sub_file.path() << "` is not exist" << std::endl;
                exit(-1);
            }

            manifest_infos[line.substr(split_index + 1, line.length())] = line.substr(0, split_index);
        }
        restore_manifest_file.close();
    }
    else
    {
        std::cerr << restore_manifest.path() << " can't open." << std::endl;
        exit(-1);
    }

    return manifest_infos;
}

void restore(const String & restore_dir_path, const std::map<String, String> & manifest_infos)
{
    for (const auto & [path, ori_path] : manifest_infos)
    {
        std::cout << "Replacing " << ori_path << std::endl;
        Poco::File ori_dir(ori_path);
        if (ori_dir.exists())
        {
            ori_dir.remove(true);
        }
        else
        {
            // Create mid path.
            ori_dir.createDirectories();
            ori_dir.remove();
        }

        std::filesystem::copy(restore_dir_path + "/" + path, ori_path, std::filesystem::copy_options::recursive);
    }
}

int main(int argc, char ** argv)
{
    if (argc != 2)
    {
        const String proc_name = String(argv[0]);
        std::cerr << "Invalid args." << std::endl;
        std::cerr << "Usage:" << std::endl;
        std::cerr << " " << proc_name << " [packed dir]" << std::endl;
        exit(-1);
    }

    String restore_dir_path = String(argv[1]);
    Poco::File restore_dir(restore_dir_path);
    if (!restore_dir.exists() || !restore_dir.isDirectory())
    {
        std::cerr << restore_dir.path() << " not existed." << std::endl;
        exit(-1);
    }

    const auto & manifest_infos = parseManifest(restore_dir_path);
    if (!askComfirm(manifest_infos))
    {
        std::cout << "User cancel." << std::endl;
        return 0;
    }

    restore(restore_dir_path, manifest_infos);
    std::cout << "All done." << std::endl;
    return 0;
}