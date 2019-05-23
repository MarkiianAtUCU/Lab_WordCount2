//
// Created by matsiuk on 22-May-19.
//

#ifndef LAB_WORDCOUNT_THREAD_SAFE_QUEUE_H
#define LAB_WORDCOUNT_THREAD_SAFE_QUEUE_H

#include  <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class thread_safe_queue {
public:
    std::queue<T> queue;
    std::mutex mtx;
    std::condition_variable cv;
    int active_workers = 0;
    int end_work = 0;
    int to_end;
    T poison_pill;
public:
    thread_safe_queue(int toEnd, T poison) : to_end(toEnd), poison_pill(poison) {}

    void push(T& elem) {
        std::unique_lock<std::mutex> lock(mtx);
        queue.push(elem);
        cv.notify_one();
    };

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx);
        return queue.empty();
    };
    bool pop(T& elem) {
        std::lock_guard<std::mutex> lock(mtx);
        if(queue.empty())
        {
            return false;
        }

        elem=queue.front();
        queue.pop();
        return true;
    };



    void wait_and_pop_two(std::pair<T,T> & elem) {
        std::unique_lock<std::mutex> lock(mtx);
        while(queue.size()<2)
        {
            cv.wait(lock);
        }
        elem.first = queue.front();
        queue.pop();
        elem.second = queue.front();
        queue.pop();
        active_workers++;
    }

    void end_of_data() {
        std::unique_lock<std::mutex> lock(mtx);
        end_work++;
        if (end_work == to_end) {
            queue.push(poison_pill);
        };
    }

//    bool should_i_wait() {
//        std::unique_lock<std::mutex> lock(mtx);
//        return !((end_work == to_end) && (queue.size() == 1) && (active_workers == 0));
//    }

//    bool is_this_end() {
//        std::unique_lock<std::mutex> lock(mtx);
//        return (end_work == to_end) && (queue.size() == 0);
//    }
//
//    void done_work() {
//        std::unique_lock<std::mutex> lock(mtx);
//        active_workers--;
//    }


    void wait_and_pop(T& elem) {
        std::unique_lock<std::mutex> lock(mtx);
        while(queue.empty())
        {
            cv.wait(lock);
        }
        elem = queue.front();
        queue.pop();
    };

    int size() {
        std::unique_lock<std::mutex> lock(mtx);
        return queue.size();
    }
};



#endif //LAB_WORDCOUNT_THREAD_SAFE_QUEUE_H
