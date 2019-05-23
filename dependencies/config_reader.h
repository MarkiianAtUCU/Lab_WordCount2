//
// Created by matsiuk on 31-Mar-19.
//

#ifndef INTEGRAL_CONFIG_READER_H
#define INTEGRAL_CONFIG_READER_H

#include <string>
#include <map>

#include <fstream>

class config {
private:
    std::string filename;
public:
    std::map<std::string, std::string> m;

    config(const std::string &filename) : filename(filename) {
        std::ifstream file(filename);
        if (file.is_open()) {
            std::string tmp_str;
            while (std::getline(file, tmp_str)) {
                int x = tmp_str.find(":");
                m[tmp_str.substr(0, x)] = tmp_str.substr(x+2, tmp_str.length());

            }
            file.close();
        } else {
            throw std::invalid_argument( "config file not found" );
        }
    }

    double get_double(std::string query) {
        try {
            return std::stod(m[query]);
        } catch (const std::invalid_argument& e) {
            throw std::invalid_argument(query+" has invalid value for double or not in config file");
        }
    }

    int get_int(std::string query) {
        try {
            return std::stoi(m[query]);
        } catch (const std::invalid_argument& e) {
            throw std::invalid_argument(query+" has invalid value for int or not in config file");
        }
    }

    std::string get_string(std::string query) {
        try {
            return m[query];
        } catch (const std::invalid_argument& e) {
            throw std::invalid_argument(query+" has invalid value for int or not in config file");
        }
    }
};

#endif //INTEGRAL_CONFIG_READER_H
