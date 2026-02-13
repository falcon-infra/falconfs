/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "read_stream/read_stream.h"

#include "buffer/open_instance.h"
#include "falcon_store/falcon_store.h"

/*---------------------- Pipe ----------------------*/

bool Pipe::Init(size_t initSize)
{
    std::unique_lock<std::mutex> xlock(mutex);
    if (mem != nullptr) {
        return true;
    }

    std::function<void(char *)> freeFunc = [](char *ptr) { MemPool::GetInstance().free(ptr); };
    mem = std::shared_ptr<char>((char *)MemPool::GetInstance().alloc(), freeFunc);
    if (mem == nullptr) {
        FALCON_LOG(LOG_ERROR) << "Pipe::Init(): malloc failed";
        return false;
    }
    capacity = initSize;
    size = initSize;
    index = initSize;
    stop = false;
    return true;
}

void Pipe::Init(size_t initSize, std::shared_ptr<char> initMem)
{
    std::unique_lock<std::mutex> xlock(mutex);
    if (mem != nullptr) {
        return;
    }
    mem = initMem;
    capacity = initSize;
    size = initSize;
    index = initSize;
    stop = false;
}

/*
 * Wait to pop data of size to buf. Mark end to caller.
 */
ssize_t Pipe::WaitPop(char *buf, size_t popSize, bool &end)
{
    std::unique_lock<std::mutex> xlock(mutex);
    /* wait for pipe not empty, or no data to push, or error */
    readCV.wait(xlock, [this]() { return index != size || size <= 0 || stop; });
    if (size < 0) {
        /* read error */
        FALCON_LOG(LOG_ERROR) << "Pipe::WaitPop(): read error : " << strerror(-size);
        auto err = size;
        Destroy();
        return err;
    }
    if (size == 0) {
        /* read end */
        FALCON_LOG(LOG_INFO) << "Pipe::WaitPop(): read to end and return";
        Destroy();
        return 0;
    }
    if (index == size && stop) {
        /* no data to push, and empty pipe */
        Destroy();
        return 0;
    }

    ssize_t readSize = std::min(popSize, (size_t)size - index);
    (void)memcpy(buf, mem.get() + index, readSize);
    index += readSize;
    if (index == size) {
        /* all data popped in pipe */
        if (stop) {
            Destroy();
        }
        end = true;
        writeCV.notify_all();
    }
    return readSize;
}

/*
 * Wait to read data of size capacity to mem.
 * Use ReadFileLR to read data, other than pass data to pipe.
 */
ssize_t Pipe::WaitPush(OpenInstance *openInstance, off_t offset)
{
    std::unique_lock<std::mutex> xlock(mutex);
    /* wait for pipe empty, or no need to push */
    writeCV.wait(xlock, [this]() { return index == size || stop; });
    if (stop) {
        readCV.notify_all();
        return 0;
    }

    ssize_t readSize = FalconStore::GetInstance()->ReadFileLR(mem.get(), offset, openInstance, capacity);
    if (readSize < 0) {
        FALCON_LOG(LOG_ERROR) << "In WaitPush(): ReadFileLR() failed";
    }

    index = 0;
    size = readSize;
    readCV.notify_all();

    return readSize;
}

void Pipe::Destroy()
{
    capacity = 0;
    size = 0;
    index = 0;
    stop = true;
    mem = nullptr;
}

/*---------------------- ReadStream ----------------------*/

/*
 * Init pipes under stream.
 */
bool ReadStream::Init(OpenInstance *instance, int blocks, size_t pipeSize)
{
    std::unique_lock<std::mutex> xlock(pipeMutex);

    openInstance = instance;
    fileBlocks = blocks;
    pipeNum = std::min(blocks, QUEUE_SIZE);
    pipeCap = pipeSize;
    stop = false;

    std::function<void(char *)> freeFunc = [](char *ptr) { MemPool::GetInstance().free(ptr); };
    std::vector<void *> mem = MemPool::GetInstance().calloc(pipeNum);
    if (mem.empty()) {
        FALCON_LOG(LOG_ERROR) << "ReadStream::Init: calloc mem failed";
        return false;
    }
    for (int i = 0; i < pipeNum; i++) {
        pipes[i].Init(pipeCap, std::shared_ptr<char>((char *)mem[i], freeFunc));
    }

    return true;
}

/*
 * Called by user.
 * Wait for data to be popped to buf.
 * If current pipe gives out less data than wanted, loop to read next pipe.
 */
ssize_t ReadStream::WaitPop(char *buf, size_t popSize)
{
    std::unique_lock<std::mutex> xlock(pipeMutex);

    ssize_t readSize = 0;
    ssize_t curSize = 0;
    while (!stop && (size_t)readSize != popSize) {
        bool isEnd = false;
        curSize = pipes[pipeIndex].WaitPop(buf + readSize, popSize - readSize, isEnd);
        if (isEnd) {
            /* next pipe */
            addPipeIndex();
        }
        if (curSize < 0) {
            /* error */
            return curSize;
        }
        if (curSize == 0) {
            /* read to end */
            break;
        }
        readSize += curSize;
    }

    return readSize;
}

/*
 * Switch the pipe that gives out data to next one
 */
void ReadStream::addPipeIndex() { pipeIndex = (pipeIndex + 1) % pipeNum; }

/*
 * Called by user.
 * Start max of threadNum and pipeNum number of threads to concurrently fills in the pipes.
 */
void ReadStream::StartPushThreaded()
{
    std::unique_lock<std::mutex> xlock(pipeMutex);
    for (int i = 0; i < pipeNum; i++) {
        threads.emplace_back([=, this]() {
            size_t offset = i * pipeCap;
            while (!stop.load() && offset < stopOffset) {
                ssize_t retSize = pipes[i].WaitPush(openInstance, offset);
                if (retSize != (ssize_t)pipes[i].capacity) {
                    /* Read data not equals capacity, means error or read end. Mark end for other reading threads */
                    /* stopOffset will be small than offset for other threads because pipes are popped in order */
                    stopOffset = offset;
                    break;
                }
                offset += pipeNum * pipeCap;
            }
            /* no data will be pushed to pipe later, wake up reader */
            pipes[i].stop = true;
            pipes[i].readCV.notify_all();
        });
    }
}

/*
 * Called by user.
 * Stop all threads pushing the pipes started by StartPushThreaded.
 * Deallocate all memory of pipe.
 */
void ReadStream::StopPushThreaded()
{
    std::unique_lock<std::mutex> xlock(pipeMutex);
    stop = true;
    for (int i = 0; i < pipeNum; i++) {
        std::unique_lock<std::mutex> pipelock(pipes[i].mutex);
        pipes[i].Destroy();
        pipes[i].writeCV.notify_all();
    }
}

void ReadStream::WaitPushEnded()
{
    for (auto &th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }
}

/*
 * Wait for all threads pushing to join
 */
ReadStream::~ReadStream()
{
    for (int i = 0; i < pipeNum; i++) {
        pipes[i].stop = true;
        pipes[i].writeCV.notify_all();
    }
    for (auto &th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }
}
