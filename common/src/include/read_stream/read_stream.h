/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once


#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#define QUEUE_SIZE 3

struct OpenInstance;

class Pipe {
  public:
    bool Init(size_t initSize);
    void Init(size_t initSize, std::shared_ptr<char> initMem);
    ssize_t WaitPop(char *buf, size_t popSize, bool &end);
    ssize_t WaitPush(OpenInstance *openInstance, off_t offset);
    void Destroy();

    std::mutex mutex;
    std::condition_variable readCV;
    std::condition_variable writeCV;
    std::shared_ptr<char> mem = nullptr;
    size_t capacity = 0;
    ssize_t size = 0;
    ssize_t index = 0;
    std::atomic<bool> stop = true;
};

class ReadStream {
  public:
    bool Init(OpenInstance *instance, int blocks, size_t pipeSize);
    ssize_t WaitPop(char *buf, size_t popSize);
    void StartPushThreaded();
    void StopPushThreaded();
    void WaitPushEnded();

    void addPipeIndex();
    ~ReadStream();

    std::atomic<size_t> stopOffset = -1;
    size_t pipeCap = 0;
    int pipeIndex = 0;
    int fileBlocks = 0;
    OpenInstance *openInstance;
    int pipeNum = 0;
    Pipe pipes[QUEUE_SIZE];
    std::mutex pipeMutex;
    std::atomic<bool> stop = true;
    std::vector<std::thread> threads;
};
