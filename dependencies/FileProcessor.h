//
// Created by matsiuk on 19-May-19.
//

#ifndef LAB_WORDCOUNT_FILEPROCESSOR_H
#define LAB_WORDCOUNT_FILEPROCESSOR_H

#include <string>
#include <vector>
#include "boost/filesystem.hpp"
#include <map>
struct FileProcessor {
    static bool is_archive(boost::filesystem::path const & entry);
    static bool is_text(boost::filesystem::path const & entry);
    static void process_file(std::string path, std::string & res_str);
    static void process_archive(std::string path, std::string & res_str);
};


struct MapProcessor {
    static void write_to_file_alphabetic(std::string path, std::map<std::string, int> & map);
    static void write_to_file_quantity(std::string path, std::map<std::string, int> & map);
    static void merge_maps(std::map<std::string, int>& res, std::map<std::string, int> & map);
};

#endif //LAB_WORDCOUNT_FILEPROCESSOR_H
