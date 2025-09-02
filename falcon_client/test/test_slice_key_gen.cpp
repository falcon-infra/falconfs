
#include <iostream>
#include <vector>
#include <set>
#include <thread>

#include "../src/include/slice_key_gen.h"


int main(){
    auto& gen = SliceKeyGenerator::getInstance();

    std::vector<uint64_t> ids;
    const int count=100;

    for (int i = 0; i< count; ++i){
        for (auto v: gen.getKeys(100)){
            ids.emplace_back(v);
        }
    }

    // 单线程检查唯一性
    std::cout << "Single thread test:\n";
    std::set<uint64_t> unique(ids.begin(), ids.end());
    if (unique.size() == ids.size()) {
        std:: cout<<"All " << ids.size() << " keys are unique with single-thread.\n";
        std:: cout << "The first 10 keys are: \n";
        for(int i = 0; i < 10; ++i) {
            std:: cout << ids[i] << " ";
        }
        std::cout << std::endl;
    }else {
        std:: cout << "ERROR: duplicated keys found with single-thread. \n";
    }

    // 多线程测试
    std::cout << "\nMultithread test:\n";
    std::vector<std::thread> threads;
    ids.clear();

    // 每个线程有自己的本地存储
    std::vector<std::vector<uint64_t>> threadLocalIds(1000);

    for (int t = 0; t < 1000; ++t) {
        threads.emplace_back([&gen, &threadLocalIds, t]() {
            auto& localIds = threadLocalIds[t]; // 线程本地vector
            for (int i = 0; i < 3; ++i) {
                for (auto v : gen.getKeys(100)) {
                    localIds.emplace_back(v); // 无锁操作
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 合并结果
    for (auto& vec : threadLocalIds) {
        ids.insert(ids.end(), vec.begin(), vec.end());
    }

    // 检查是否有重复 ID
    std::set<uint64_t> unique_multithread(ids.begin(), ids.end());
    if (unique_multithread.size() == ids.size()) {
        std:: cout<<"All " << ids.size() << " keys are unique with multi-thread.\n";
        std:: cout << "The first 10 keys are: \n";
        for(int i = 0; i < 10; ++i) {
            std:: cout << ids[i] << " ";
        }
        std::cout << std::endl;
    }else {
        std:: cout << "ERROR: duplicated keys found with multi-thread. \n";
    }

    return 0;
}