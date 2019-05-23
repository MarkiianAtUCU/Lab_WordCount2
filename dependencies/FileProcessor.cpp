#include "FileProcessor.h"
#include <string>
#include "archive.h"
#include <sstream>
#include "archive_entry.h"
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <exception>

void FileProcessor::process_archive(std::string path, std::string & res_str) {
    struct archive *a;
    struct archive_entry *entry;
    int r;

    a = archive_read_new();
    archive_read_support_filter_all(a);
    archive_read_support_format_all(a);
    r = archive_read_open_filename(a, path.c_str(), 10240); // Note 1
    if (r != ARCHIVE_OK)
        throw std::invalid_argument("Archive open error");
    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {

//        printf("%s - is file: %i\n", archive_entry_pathname(entry), archive_entry_filetype(entry)==AE_IFREG);
        if (archive_entry_filetype(entry) == AE_IFREG) {
            auto *buff = new char[archive_entry_size(entry)];
            archive_read_data(a, buff, archive_entry_size(entry));
            res_str = buff;
            delete[] buff;
        }
    }
    r = archive_read_free(a);  // Note 3
    if (r != ARCHIVE_OK)
        throw std::invalid_argument("Archive free error");
}
void FileProcessor::process_file(std::string path, std::string & res_str) {
    std::ifstream in(path);
    auto ss = std::ostringstream{};
    ss << in.rdbuf();
    res_str = ss.str();
}

bool FileProcessor::is_archive(boost::filesystem::path const & entry) {
    std::vector<std::string> extensions = {".zip", ".tar", ".gz", ".tar.gz", ".7z"};
    for (auto &ext: extensions) {
        if (boost::iequals(boost::filesystem::extension(entry), ext)) {
            return true;
        }
    }
    return false;
}
bool FileProcessor::is_text(boost::filesystem::path const & entry) {
    return !FileProcessor::is_archive(entry) && boost::filesystem::is_regular_file(entry);
}

void MapProcessor::write_to_file_alphabetic(std::string path, std::map<std::string, int> &map) {
    std::ofstream file(path);
    for (auto & entry : map) {
        file << std::setw(25) << entry.first <<  "  :  " <<entry.second << std::endl;
    }
}
void MapProcessor::write_to_file_quantity(std::string path, std::map<std::string, int> &map) {
    std::vector<std::pair<std::string, int>> tmp_v;
    std::copy(map.begin(), map.end(), std::back_inserter(tmp_v));
    std::sort(tmp_v.begin(), tmp_v.end(), [](std::pair<std::string, int> const &first, std::pair<std::string, int> const & second) { return first.second > second.second;});
    std::ofstream file(path);
    for (auto & entry : tmp_v) {
        file << std::setw(25) << entry.first <<  "  :  " <<entry.second << std::endl;
    }
}

void MapProcessor::merge_maps(std::map<std::string, int> &res, std::map<std::string, int> &map) {
    for (auto & entry : map) {
        res[entry.first] += entry.second;
    }
}
