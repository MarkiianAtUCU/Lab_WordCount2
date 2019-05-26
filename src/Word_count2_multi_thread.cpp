#include <iostream>
#include "boost/filesystem.hpp"
#include "../dependencies/FileProcessor.h"
#include <vector>
#include "boost/locale.hpp"
#include "../dependencies/time_meter.h"
#include "../dependencies/config_reader.h"
#include <thread>
#include <mutex>
#include <boost/iterator/distance.hpp>
#include "../dependencies/thread_safe_queue.h"
#include <chrono>

using sp_string = std::shared_ptr<std::string>;
using tsq_string = thread_safe_queue<sp_string >;

using sp_path = std::shared_ptr<boost::filesystem::path>;
using tsq_path = thread_safe_queue<sp_path>;

using sp_map = std::shared_ptr<std::map<std::string, int>>;
using tsq_map = thread_safe_queue<sp_map>;

sp_path poison_path(nullptr);
sp_string poison_string(nullptr);
sp_map poison_map(nullptr);

void worker_reader(tsq_path & all_files, tsq_string & results) {
    for(;;) {
        sp_path elem_ptr;
        all_files.wait_and_pop(elem_ptr);
        if (elem_ptr == poison_path) {
            all_files.push(poison_path);
            break;
        }
        auto elem = *elem_ptr;
        std::string res;
        if (FileProcessor::is_archive(elem)) {
            FileProcessor::process_archive(elem.string(), res);
            sp_string sp = std::make_shared<std::string>(res);
            results.push(sp);
        } else if (FileProcessor::is_text(elem)) {
            FileProcessor::process_file(elem.string(), res);
            sp_string sp = std::make_shared<std::string>(res);
            results.push(sp);
        }
    }
    std::cout << "END READ" <<std::endl;
    results.push(poison_string);
}

void worker_processor(tsq_string & strings, tsq_map & queue_res) {
    for (;;) {
        sp_string elem_ptr;
        strings.wait_and_pop(elem_ptr);
        if (elem_ptr == poison_string) {
            strings.push(poison_string);
            break;
        }
        std::string elem = *elem_ptr;
        elem =  boost::locale::normalize(elem);
        boost::locale::boundary::ssegment_index map(boost::locale::boundary::word,elem.begin(),elem.end());
        map.rule(boost::locale::boundary::word_letters);
        elem.clear();
        std::map<std::string, int> thread_res_map;

        for(boost::locale::boundary::ssegment_index::iterator it=map.begin(),e=map.end(); it!=e; ++it) {
            thread_res_map[boost::locale::fold_case(it -> str())]++;
        }
        sp_map sp = std::make_shared<std::map<std::string, int>>(thread_res_map);
        queue_res.push(sp);
    }

    std::cout << "END PROCESS" << std::endl;
    queue_res.end_of_data();
}

void worker_merger(tsq_map & queue_maps, tsq_map & res_merging) {
    std::map<std::string, int> res;

    for (;;) {
        sp_map elem_ptr;
        queue_maps.wait_and_pop(elem_ptr);

        if (elem_ptr == poison_map) {
            queue_maps.push(poison_map);
            break;
        }
        std::map<std::string, int> elem = *elem_ptr;
        MapProcessor::merge_maps(res, elem);
    }

    sp_map sp = std::make_shared<std::map<std::string, int>>(res);
    res_merging.push(sp);
    res_merging.end_of_data();
}

void worker_merger_final(tsq_map & queue_maps, std::map<std::string, int> & res) {
    for (;;) {
        sp_map elem_ptr;
        queue_maps.wait_and_pop(elem_ptr);
        if (elem_ptr == poison_map) {
            break;
        }
        MapProcessor::merge_maps(res, *elem_ptr);
    }
}

int main(int argc, char **argv) {
    if (argc != 2) {
        try {
            auto config_object = config("./config.dat");
        } catch (...) {
            std::cout << "Incorrect number of arguments:" << std::endl << "./Word_count2_multi_thread <config path>" <<std::endl;
            exit(1);
        }
    }
    auto start = get_current_time_fenced();
    boost::locale::generator gen;
    std::locale loc = gen("");
    std::locale::global(loc);
    std::wcout.imbue(loc);
    std::ios_base::sync_with_stdio(false);

    auto config_object = config(argv[1]);
    std::string in_file = config_object.get_string("in_file");
    in_file.erase(std::remove(in_file.begin(), in_file.end(), '\r'), in_file.end());
    std::string out_file_a = config_object.get_string("out_file_a");
    std::string out_file_n = config_object.get_string("out_file_n");


    int reader_num = config_object.get_int("num_threads_processing");
    int merger_num = config_object.get_int("num_threads_merging");
    std::vector<std::thread> all_threads;


    tsq_path  queue_path(0, poison_path);
    tsq_string queue_readed_data(0, poison_string);
    tsq_map res_map(reader_num, poison_map);
    tsq_map res_merging(merger_num, poison_map);
    std::map<std::string, int> final_res;

    all_threads.push_back(std::thread(worker_reader, std::ref(queue_path), std::ref(queue_readed_data)));

    for (int i = 0; i<reader_num ;i++) {
        all_threads.push_back(std::thread(worker_processor, std::ref(queue_readed_data), std::ref(res_map)));
    }

    for (int i = 0; i<merger_num ;i++) {
        all_threads.push_back(std::thread(worker_merger, std::ref(res_map), std::ref(res_merging)));
    }

    all_threads.push_back(std::thread(worker_merger_final, std::ref(res_merging), std::ref(final_res)));


    for (boost::filesystem::directory_entry& entry : boost::filesystem::recursive_directory_iterator(in_file)) {
        auto sp = std::make_shared<boost::filesystem::path>(entry.path());
        queue_path.push(sp);
    }
    queue_path.push(poison_path);

    for (auto & elem: all_threads) {
        elem.join();
    }


    MapProcessor::write_to_file_alphabetic(out_file_a, final_res);
    MapProcessor::write_to_file_quantity(out_file_n, final_res);
    auto finish = get_current_time_fenced();

    long long time = to_us(finish-start);
    std::cout << "T: " << time << "us" << std::endl;
    std::cout << "T: " << time/1000000 << "s" << std::endl;

    return 0;

}
