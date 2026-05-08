#pragma GCC diagnostic ignored "-Wmissing-field-initializers"

#include <Python.h>

#include <atomic>
#include <condition_variable>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <string>
#include <unistd.h>

#include "conf/falcon_property_key.h"
#include "error_code.h"
#include "falcon_code.h"
#include "falcon_meta.h"
#include "init/falcon_init.h"
#include "log/logging.h"
#include "stats/falcon_stats.h"

/* =================== Timing Helpers =======================*/
static int64_t steady_clock_now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

/* =================== Blocking Methods =======================*/
static void Init(const char* workspace, const char* runningConfigFile) 
{
    const char* baseConfigFile = runningConfigFile;
    
    std::ifstream baseConfig(baseConfigFile, std::ios::in);
    if (!baseConfig.is_open())
        throw std::runtime_error("CONFIG_FILE cannot be opened.");
    std::string pyConfigFile = std::string(workspace) + "/pyfalcon_config.json";
    std::ofstream pyConfig(pyConfigFile, std::ios::out);
    if (!pyConfig.is_open()) 
    {
        baseConfig.close();
        throw std::runtime_error("target(" + pyConfigFile + ") cannot be opened.");
    }

    std::string line;
    while (getline(baseConfig, line)) 
    {
        size_t pos = line.find("falcon_log_dir");
        if (pos != std::string::npos)
        {
            pyConfig << line.substr(0, pos) << "falcon_log_dir\": \"" << workspace << "\",";
        }
        else
        {
            pyConfig << line;
        }

        pyConfig << '\n';
    }

    baseConfig.close();
    pyConfig.close();

    setenv("CONFIG_FILE", pyConfigFile.c_str(), 1);

    int ret = -1;
    ret = GetInit().Init();
    if (ret != FALCON_SUCCESS)
        throw std::runtime_error("Falcon init failed. Error: " + std::to_string(ret));
    auto &config = GetInit().GetFalconConfig();
    std::string serverIp = config->GetString(FalconPropertyKey::FALCON_SERVER_IP);
    std::string serverPort = config->GetString(FalconPropertyKey::FALCON_SERVER_PORT);
    ret = FalconInit(serverIp, std::stoi(serverPort));
    if (ret != FALCON_SUCCESS)
        throw std::runtime_error("Falcon cluster failed." + std::to_string(ret));
}
static PyObject* PyWrapper_Init(PyObject* self, PyObject* args) 
{
    char* workspace = nullptr;
    char* runningConfigFile = nullptr;
    if (!PyArg_ParseTuple(args, "ss", &workspace, &runningConfigFile))
        return NULL;
    
    try
    {
        Init(workspace, runningConfigFile);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    Py_RETURN_NONE;
}

static int Mkdir(const char* path)
{
    FalconStats::GetInstance().stats[META_MKDIR].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconMkdir(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
} 
static PyObject* PyWrapper_Mkdir(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = Mkdir(path);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Rmdir(const char* path)
{
    FalconStats::GetInstance().stats[META_RMDIR].fetch_add(1);
    StatFuseTimer t;
    int ret = -1;
    ret = FalconRmDir(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
} 
static PyObject* PyWrapper_Rmdir(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = Rmdir(path);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Create(const char *path, int oflags, uint64_t& fd)
{
    std::string pathStr = path;

    FalconStats::GetInstance().stats[META_CREATE].fetch_add(1);
    StatFuseTimer t;
    struct stat st;
    (void)memset(&st, 0, sizeof(st));
    int ret = FalconCreate(pathStr, fd, oflags, &st);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Create(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    int oflags = 0;
    if (!PyArg_ParseTuple(args, "si", &path, &oflags))
        return NULL;
    
    int ret = -1;
    uint64_t fd;
    try
    {
        ret = Create(path, oflags, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return Py_BuildValue("(iK)", ret, fd);
}

static int Unlink(const char *path)
{
    FalconStats::GetInstance().stats[META_UNLINK].fetch_add(1);
    StatFuseTimer t;
    int ret = -1;
    ret = FalconUnlink(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Unlink(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = Unlink(path);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Open(const char *path, int oflags, uint64_t& fd)
{
    FalconStats::GetInstance().stats[META_OPEN].fetch_add(1);
    StatFuseTimer t;
    struct stat st;
    (void)memset(&st, 0, sizeof(st));
    int ret = FalconOpen(path, oflags, fd, &st);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Open(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    int oflags = 0;
    if (!PyArg_ParseTuple(args, "si", &path, &oflags))
        return NULL;
    
    int ret = -1;
    uint64_t fd;
    try
    {
        ret = Open(path, oflags, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return Py_BuildValue("(iK)", ret, fd);
}

static int Flush(const char *path, uint64_t fd)
{
    FalconStats::GetInstance().stats[META_FLUSH].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconClose(path, fd, true, -1);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Flush(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    uint64_t fd;
    if (!PyArg_ParseTuple(args, "sK", &path, &fd))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = Flush(path, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Close(const char *path, uint64_t fd)
{
    FalconStats::GetInstance().stats[META_FLUSH].fetch_add(1);
    FalconStats::GetInstance().stats[META_RELEASE].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconClose(path, fd, false, -1);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Close(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    uint64_t fd;
    if (!PyArg_ParseTuple(args, "sK", &path, &fd))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = Close(path, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Read(const char *path, uint64_t fd, char *buffer, size_t size, off_t offset)
{
    FalconStats::GetInstance().stats[FUSE_READ_OPS].fetch_add(1);
    StatFuseTimer t(FUSE_READ_LAT);
    int retSize = FalconRead(path, fd, buffer, size, offset);
    FalconStats::GetInstance().stats[FUSE_READ] += retSize >= 0 ? retSize : 0;
    return retSize;
}
static PyObject* PyWrapper_Read(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    uint64_t fd;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sKw*ii", &path, &fd, &buffer, &size, &offset))
        return NULL;
    if (buffer.len < size)
    {
        PyErr_SetString(PyExc_RuntimeError, "the buffer is not enough for requested data.");
        return NULL;
    }
    
    int ret = -1;
    try
    {
        ret = Read(path, fd, (char*)buffer.buf, size, offset);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Write(const char *path, uint64_t fd, char *buffer, size_t size, off_t offset)
{
    FalconStats::GetInstance().stats[FUSE_WRITE_OPS].fetch_add(1);
    StatFuseTimer t(FUSE_WRITE_LAT);
    uint ret = -1;
    ret = FalconWrite(fd, path, buffer, size, offset);
    if (ret != 0) {
        return ret;
    }
    FalconStats::GetInstance().stats[FUSE_WRITE] += size;
    return ret;
}
static PyObject* PyWrapper_Write(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    uint64_t fd;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sKw*ii", &path, &fd, &buffer, &size, &offset))
        return NULL;
    if (buffer.len < size)
    {
        PyErr_SetString(PyExc_RuntimeError, "the buffer is not enough for writing data.");
        return NULL;
    }
    
    int ret = -1;
    try
    {
        ret = Write(path, fd, (char*)buffer.buf, size, offset);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int Stat(const char *path, struct stat *stbuf)
{
    const char *lastSlash = strrchr(path, '/');
    if (lastSlash != nullptr && *(lastSlash + 1) == 1) 
    {
        char middle_component_flag = *(lastSlash + 2);

        FalconStats::GetInstance().stats[META_LOOKUP].fetch_add(1);
        (void)memmove((char *)(lastSlash + 1),
                      (char *)(lastSlash + 3),
                      strlen(lastSlash + 3) + 1);
        if (middle_component_flag == '1') 
        {
            stbuf->st_mode = 040777;
            return 0;
        }
    } 
    else 
    {
        FalconStats::GetInstance().stats[META_STAT].fetch_add(1);
    }

    StatFuseTimer t;
    int ret = FalconGetStat(path, stbuf);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_Stat(PyObject* self, PyObject* args)
{
    // T_entry: C++ function entry timestamp (GIL already held by Python)
    int64_t entry_us = steady_clock_now_us();

    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    int ret = -1;
    struct stat stbuf;
    try
    {
        ret = Stat(path, &stbuf);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    // T_exit: C++ function about to return to Python (GIL still held)
    int64_t exit_us = steady_clock_now_us();

    PyObject* dict = PyDict_New();
    if (ret == 0)
    {
        PyDict_SetItem(dict, PyUnicode_FromString("st_dev"), PyLong_FromLong(stbuf.st_dev));
        PyDict_SetItem(dict, PyUnicode_FromString("st_ino"), PyLong_FromLong(stbuf.st_ino));
        PyDict_SetItem(dict, PyUnicode_FromString("st_nlink"), PyLong_FromLong(stbuf.st_nlink));
        PyDict_SetItem(dict, PyUnicode_FromString("st_mode"), PyLong_FromLong(stbuf.st_mode));
        PyDict_SetItem(dict, PyUnicode_FromString("st_uid"), PyLong_FromLong(stbuf.st_uid));
        PyDict_SetItem(dict, PyUnicode_FromString("st_gid"), PyLong_FromLong(stbuf.st_gid));
        PyDict_SetItem(dict, PyUnicode_FromString("st_rdev"), PyLong_FromLong(stbuf.st_rdev));
        PyDict_SetItem(dict, PyUnicode_FromString("st_size"), PyLong_FromLong(stbuf.st_size));
        PyDict_SetItem(dict, PyUnicode_FromString("st_blksize"), PyLong_FromLong(stbuf.st_blksize));
        PyDict_SetItem(dict, PyUnicode_FromString("st_blocks"), PyLong_FromLong(stbuf.st_blocks));
        PyDict_SetItem(dict, PyUnicode_FromString("st_atime"), PyLong_FromLong(stbuf.st_atime));
        PyDict_SetItem(dict, PyUnicode_FromString("st_mtime"), PyLong_FromLong(stbuf.st_mtime));
        PyDict_SetItem(dict, PyUnicode_FromString("st_ctime"), PyLong_FromLong(stbuf.st_ctime));
    }

    // Return 4-tuple: (ret, dict, entry_us, exit_us)
    return Py_BuildValue("(iNLL)", ret, dict, entry_us, exit_us);
}

static int OpenDir(const char *path, uint64_t& fd)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_OPENDIR].fetch_add(1);
    StatFuseTimer t;
    FalconFuseInfo fi;
    int ret = FalconOpenDir(path, &fi);
    fd = fi.fh;
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_OpenDir(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;
    
    int ret = -1;
    uint64_t fd = 0;
    try
    {
        ret = OpenDir(path, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    return Py_BuildValue("(iK)", ret, fd);
}

static int CloseDir(const char *path, uint64_t fd)
{
    FalconStats::GetInstance().stats[META_RELEASEDIR].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconCloseDir(fd);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_CloseDir(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    uint64_t fd;
    if (!PyArg_ParseTuple(args, "sK", &path, &fd))
        return NULL;
    
    int ret = -1;
    try
    {
        ret = CloseDir(path, fd);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
    
    return PyLong_FromLong(ret);
}

static int ReadDir(const char *path, void *buf, FalconFuseFiller filler, off_t offset, uint64_t fd)
{
    FalconStats::GetInstance().stats[META_READDIR].fetch_add(1);
    StatFuseTimer t;
    int ret = 0;
    FalconFuseInfo fi;
    fi.fh = fd;
    ret = FalconReadDir(path, buf, filler, offset, &fi);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}
static PyObject* PyWrapper_ReadDir(PyObject* self, PyObject* args)
{
    char* path = nullptr;
    uint64_t fd = 0;
    if (!PyArg_ParseTuple(args, "sK", &path, &fd))
        return NULL;
    
    PyObject* list = PyList_New(0);
    auto filler = [](void* buf, const char* name, const struct stat* stbuf, off_t index) -> int
    {
        PyObject* list = (PyObject*)buf;
        mode_t mode = stbuf ? stbuf->st_mode : (S_IFDIR | 0755);
        PyList_Append(list, Py_BuildValue("(si)", name, mode));
        return 0;
    };
    int ret = 0;
    int offset = 0;
    while (true)
    {
        ret = ReadDir(path, list, filler, offset, fd);
        if (ret != 0)
            break;
        int newOffset = PyList_Size(list);
        if (newOffset == offset)
            break;
        offset = newOffset;
    }

    return Py_BuildValue("(iN)", ret, list);
}

/* =================== Non-Blocking Methods =======================*/

// Priority constants: lower value = higher priority
constexpr int TASK_PRIORITY_GET = 0;
constexpr int TASK_PRIORITY_PUT = 10;

class AsyncTaskThreadPool
{
private:
    struct TaskItem {
        int priority;
        uint64_t sequence;  // FIFO within same priority
        std::function<void()> task;

        bool operator<(const TaskItem& other) const {
            if (priority != other.priority)
                return priority > other.priority;  // lower value = higher priority
            return sequence > other.sequence;       // FIFO within same priority
        }
    };

    std::priority_queue<TaskItem> taskQueue_;
    std::mutex queueMutex_;
    std::condition_variable queueCV_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    std::atomic<uint64_t> sequenceCounter_{0};
    size_t numWorkers_;

    void workerLoop() {
        while (true) {
            TaskItem item;
            {
                std::unique_lock<std::mutex> lock(queueMutex_);
                queueCV_.wait(lock, [this] {
                    return stop_.load() || !taskQueue_.empty();
                });
                if (stop_.load() && taskQueue_.empty())
                    return;
                item = std::move(taskQueue_.top());
                taskQueue_.pop();
            }
            item.task();
        }
    }

public:
    explicit AsyncTaskThreadPool(size_t numWorkers)
        : numWorkers_(numWorkers)
    {
        for (size_t i = 0; i < numWorkers; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    ~AsyncTaskThreadPool() {
        shutdown();
    }

    // Dispatch with priority, returns future for awaiting result
    template<class F, class... Args>
    auto DispatchWithPriority(int priority, F&& f, Args&&... args)
        -> std::future<decltype(f(args...))>
    {
        using return_type = decltype(f(args...));
        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        std::future<return_type> res = task->get_future();

        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            taskQueue_.push(TaskItem{
                priority,
                sequenceCounter_.fetch_add(1),
                [task]() { (*task)(); }
            });
        }
        queueCV_.notify_one();
        return res;
    }

    // Fire-and-forget: no future returned, for PUT tasks
    template<class F, class... Args>
    void DispatchFireAndForget(int priority, F&& f, Args&&... args)
    {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            taskQueue_.push(TaskItem{
                priority,
                sequenceCounter_.fetch_add(1),
                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
            });
        }
        queueCV_.notify_one();
    }

    void shutdown() {
        bool expected = false;
        if (!stop_.compare_exchange_strong(expected, true))
            return;  // already shut down
        queueCV_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable())
                w.join();
        }
        workers_.clear();
    }
};

static std::mutex AsyncTaskThreadPoolForPyMutex;
static std::shared_ptr<AsyncTaskThreadPool> AsyncTaskThreadPoolForPy = nullptr;

class AsyncResultBase
{
public:
    char* exceptionInfo;
    AsyncResultBase() : exceptionInfo(nullptr) { }
    AsyncResultBase(char* exceptionInfo) : exceptionInfo(exceptionInfo) { }

    virtual PyObject* GeneratePyObject()
    {
        PyErr_SetString(PyExc_RuntimeError, exceptionInfo);
        return NULL;
    }
    ~AsyncResultBase()
    {
        if (exceptionInfo)
            free(exceptionInfo);
    }
};
struct AsyncState
{
    PyObject_HEAD
    std::future<std::unique_ptr<AsyncResultBase>> future;
    int64_t cpp_recv_time_us = 0;
    int64_t cpp_done_time_us = 0;
};

class AsyncResultIntOnly : public AsyncResultBase
{
public:
    int data = 0;

    AsyncResultIntOnly(int data) : data(data) { }
    PyObject* GeneratePyObject()
    {
        if (exceptionInfo)
            return AsyncResultBase::GeneratePyObject();
        return PyLong_FromLong(data);
    }
};

class AsyncResultReadSize : public AsyncResultBase
{
public:
    int bytesRead;
    AsyncResultReadSize(int bytesRead) : bytesRead(bytesRead) {}
    PyObject* GeneratePyObject() override
    {
        if (exceptionInfo)
            return AsyncResultBase::GeneratePyObject();
        return PyLong_FromLong(bytesRead);
    }
};

static PyObject* AsyncState_new(PyTypeObject* type, PyObject* args, PyObject* kwargs) 
{
    AsyncState* self = (AsyncState*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static void AsyncState_dealloc(AsyncState* self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* AsyncState_get_cpp_recv_time(AsyncState* self, void* closure) {
    return PyLong_FromLongLong(self->cpp_recv_time_us);
}

static PyObject* AsyncState_get_cpp_done_time(AsyncState* self, void* closure) {
    return PyLong_FromLongLong(self->cpp_done_time_us);
}

static PyGetSetDef AsyncState_getsetters[] = {
    {"cpp_recv_time_us", (getter)AsyncState_get_cpp_recv_time, NULL,
     "C++ worker thread receive timestamp (microseconds since steady_clock epoch)", NULL},
    {"cpp_done_time_us", (getter)AsyncState_get_cpp_done_time, NULL,
     "C++ worker thread done timestamp (microseconds since steady_clock epoch)", NULL},
    {NULL}
};

static PyObject* AsyncState_iter(PyObject* self) 
{
    Py_INCREF(self);
    return self;
}

static PyObject* AsyncState_iternext(PyObject* self) 
{
    AsyncState* state = (AsyncState*)self;
    if (state->future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
        Py_RETURN_NONE;

    std::unique_ptr<AsyncResultBase> result = state->future.get();
    PyObject* pyResult = result->GeneratePyObject();
    PyErr_SetObject(PyExc_StopIteration, pyResult);
    Py_DECREF(pyResult);
    return NULL;
}

static PyObject* AsyncState_await(PyObject *self)
{
    Py_INCREF(self);
    return self;
}

static PyAsyncMethods AsyncState_as_async = 
{
    .am_await = AsyncState_await,
    .am_aiter = AsyncState_iter,
    .am_anext = AsyncState_iternext
};

static PyTypeObject AsyncStateType =
{
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pyfalconfs.AsyncState",
    .tp_basicsize = sizeof(AsyncState),
    .tp_dealloc = (destructor)AsyncState_dealloc,
    .tp_as_async = &AsyncState_as_async,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Asynchronous task state",
    .tp_iter = AsyncState_iter,
    .tp_iternext = AsyncState_iternext,
    .tp_getset = AsyncState_getsetters,
    .tp_new = AsyncState_new,
};

static PyObject* PyWrapper_AsyncExists(PyObject* self, PyObject* args)
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path))
        return nullptr;

    AsyncState* state = (AsyncState*)AsyncStateType.tp_new(&AsyncStateType, nullptr, nullptr);
    state->cpp_recv_time_us = steady_clock_now_us();
    auto task = [path, state]() -> std::unique_ptr<AsyncResultBase>
    {
        int ret = -1;
        struct stat stbuf;
        try
        {
            ret = Stat(path, &stbuf);
        }
        catch (const std::exception& e)
        {
            state->cpp_done_time_us = steady_clock_now_us();
            return std::make_unique<AsyncResultBase>(strdup(e.what()));
        }

        state->cpp_done_time_us = steady_clock_now_us();
        return std::make_unique<AsyncResultIntOnly>(ret);
    };
    state->future = AsyncTaskThreadPoolForPy->DispatchWithPriority(TASK_PRIORITY_GET, task);
    return (PyObject*)state;
}

static PyObject* PyWrapper_AsyncGet(PyObject* self, PyObject* args)
{
    char* path = nullptr;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sw*ii", &path, &buffer, &size, &offset))
        return nullptr;

    AsyncState *state = (AsyncState *)AsyncStateType.tp_new(&AsyncStateType, nullptr, nullptr);
    state->cpp_recv_time_us = steady_clock_now_us();
    auto task = [path, buffer, size, offset, state]() -> std::unique_ptr<AsyncResultBase> {
        int ret = -1;
        int readSize;
        uint64_t fd = UINT64_MAX;
        try
        {
            ret = Open(path, O_RDONLY, fd);
            if (ret != 0)
            {
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }

            readSize = Read(path, fd, (char*)buffer.buf, size, offset);
            if (readSize < 0)
            {
                Close(path, fd);
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(readSize);
            }

            ret = Close(path, fd);
            if (ret != 0)
            {
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }
        }
        catch (const std::exception& e)
        {
            if (fd != UINT64_MAX)
                Close(path, fd);
            state->cpp_done_time_us = steady_clock_now_us();
            return std::make_unique<AsyncResultBase>(strdup(e.what()));
        }
        state->cpp_done_time_us = steady_clock_now_us();
        return std::make_unique<AsyncResultReadSize>(readSize);
    };
    state->future = AsyncTaskThreadPoolForPy->DispatchWithPriority(TASK_PRIORITY_GET, task);
    return (PyObject*)state;
}

static PyObject* PyWrapper_AsyncPut(PyObject* self, PyObject* args)
{
    char* path = nullptr;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sw*ii", &path, &buffer, &size, &offset))
        return nullptr;

    AsyncState *state = (AsyncState *)AsyncStateType.tp_new(&AsyncStateType, nullptr, nullptr);
    state->cpp_recv_time_us = steady_clock_now_us();
    auto task = [path, buffer, size, offset, state]() -> std::unique_ptr<AsyncResultBase>
    {
        int ret = -1;
        uint64_t fd = UINT64_MAX;
        try
        {
            ret = Create(path, O_CREAT | O_WRONLY | O_TRUNC, fd);
            if (ret != 0)
            {
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }

            ret = Write(path, fd, (char*)buffer.buf, size, offset);
            if (ret != 0)
            {
                Close(path, fd);
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }

            ret = Flush(path, fd);
            if (ret != 0)
            {
                Close(path, fd);
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }

            ret = Close(path, fd);
            if (ret != 0)
            {
                state->cpp_done_time_us = steady_clock_now_us();
                return std::make_unique<AsyncResultIntOnly>(ret);
            }
        }
        catch (const std::exception& e)
        {
            if (fd != UINT64_MAX)
                Close(path, fd);
            state->cpp_done_time_us = steady_clock_now_us();
            return std::make_unique<AsyncResultBase>(strdup(e.what()));
        }
        state->cpp_done_time_us = steady_clock_now_us();
        return std::make_unique<AsyncResultIntOnly>(ret);
    };
    state->future = AsyncTaskThreadPoolForPy->DispatchWithPriority(TASK_PRIORITY_PUT, task);
    return (PyObject*)state;
}

static PyObject* PyWrapper_AsyncPutNoWait(PyObject* self, PyObject* args)
{
    char* path = nullptr;
    Py_buffer buffer;
    int size;
    int offset;
    PyObject* memobj = nullptr;
    if (!PyArg_ParseTuple(args, "sy*iiO", &path, &buffer, &size, &offset, &memobj))
        return nullptr;

    // 0-copy: only take the raw pointer, no memcpy
    char* dataPtr = (char*)buffer.buf;
    std::string pathStr(path);

    // Keep both Python objects alive until async task completes
    PyObject* bufObj = buffer.obj;
    Py_INCREF(bufObj);   // prevents memoryview from being GC'd
    Py_INCREF(memobj);   // prevents MemoryObj from being GC'd

    // Release the buffer view (we already have the raw pointer)
    PyBuffer_Release(&buffer);

    AsyncTaskThreadPoolForPy->DispatchFireAndForget(
        TASK_PRIORITY_PUT,
        [pathStr, dataPtr, size, offset, bufObj, memobj]() -> void {
            uint64_t fd = UINT64_MAX;
            try {
                int ret = Create(pathStr.c_str(), O_CREAT | O_WRONLY | O_TRUNC, fd);
                if (ret != 0) goto done;

                ret = Write(pathStr.c_str(), fd, dataPtr, size, offset);
                if (ret != 0) {
                    Close(pathStr.c_str(), fd);
                    goto done;
                }

                ret = Flush(pathStr.c_str(), fd);
                Close(pathStr.c_str(), fd);
            } catch (...) {
                if (fd != UINT64_MAX)
                    Close(pathStr.c_str(), fd);
            }

        done:
            // Cleanup: acquire GIL to release Python references
            PyGILState_STATE gstate = PyGILState_Ensure();
            // Call ref_count_down to allow memory pool to reclaim
            PyObject* r = PyObject_CallMethod(memobj, "ref_count_down", NULL);
            Py_XDECREF(r);
            Py_DECREF(memobj);
            Py_DECREF(bufObj);
            PyGILState_Release(gstate);
        }
    );

    Py_RETURN_NONE;
}

static PyObject* PyWrapper_GetSteadyClockUs(PyObject* self, PyObject* args)
{
    return PyLong_FromLongLong(steady_clock_now_us());
}

static PyMethodDef PyFalconFSInternalMethods[] =
{
    {
        "Init", 
        PyWrapper_Init, 
        METH_VARARGS, 
        "Initialize FalconFS with workspace and running config file\n"
        "Parameters:\n"
        "  workspace (str): Workspace for python falconfs client\n"
        "  running_config_file (str): Configuration file path of running fuse-based falconfs\n"
        "Returns:\n"
        "  NONE"
    },
    {
        "Mkdir", 
        PyWrapper_Mkdir, 
        METH_VARARGS, 
        "Create directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "Rmdir", 
        PyWrapper_Rmdir, 
        METH_VARARGS, 
        "Remove directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "Create", 
        PyWrapper_Create, 
        METH_VARARGS, 
        "Create directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  oflags (int): Mode of created file\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  fd (int): File descriptor of created file"
    },
    {
        "Unlink", 
        PyWrapper_Unlink, 
        METH_VARARGS, 
        "Remove file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "Open", 
        PyWrapper_Open, 
        METH_VARARGS, 
        "Open file\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  oflags (int): Mode of opened file\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  fd (int): File descriptor of opened file"
    },
    {
        "Flush", 
        PyWrapper_Flush, 
        METH_VARARGS, 
        "Flush file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target file\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "Close", 
        PyWrapper_Close, 
        METH_VARARGS, 
        "Close file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target file\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "Read", 
        PyWrapper_Read, 
        METH_VARARGS, 
        "Read file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target file\n"
        "  buffer (bytearray): Space to store data\n"
        "  size (int): Requested size\n"
        "  offset (int): Read offset\n"
        "Returns:\n"
        "  read size (int): read byte size"
    },
    {
        "Write", 
        PyWrapper_Write, 
        METH_VARARGS, 
        "Write data to file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target file\n"
        "  buffer (bytearray): Space of stored data\n"
        "  size (int): To write size\n"
        "  offset (int): Write offset\n"
        "Returns:\n"
        "  write size (int): write byte size"
    },
    {
        "Stat",
        PyWrapper_Stat,
        METH_VARARGS,
        "Get file/directory status in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file/directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  stbuf (dict): Info of target\n"
        "  entry_us (int): C++ function entry timestamp (steady_clock microseconds)\n"
        "  exit_us (int): C++ function exit timestamp (steady_clock microseconds)"
    },
    {
        "OpenDir", 
        PyWrapper_OpenDir, 
        METH_VARARGS, 
        "Open directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  fd (int): File descriptor of opened directory"
    },
    {
        "CloseDir", 
        PyWrapper_CloseDir, 
        METH_VARARGS, 
        "Close directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target directory path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target directory\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "ReadDir", 
        PyWrapper_ReadDir, 
        METH_VARARGS, 
        "Read directory in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target directory path, must start with '/', which corresponding to mount point\n"
        "  fd (int): File descriptor of target directory\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  content (list): Contain items which are (name, st_mode)"
    },
    {
        "AsyncExists", 
        PyWrapper_AsyncExists, 
        METH_VARARGS, 
        "Check file/directory exists in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file/directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "AsyncGet", 
        PyWrapper_AsyncGet, 
        METH_VARARGS, 
        "Get file content in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  buffer (bytearray): Space to store data\n"
        "  size (int): Requested size\n"
        "  offset (int): Read offset\n"
        "Returns:\n"
        "  read size (int): read byte size"
    },
    {
        "AsyncPut",
        PyWrapper_AsyncPut,
        METH_VARARGS,
        "Put data to file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  buffer (bytearray): Space of stored data\n"
        "  size (int): To write size\n"
        "  offset (int): Write offset\n"
        "Returns:\n"
        "  write size (int): write byte size"
    },
    {
        "AsyncPutNoWait",
        PyWrapper_AsyncPutNoWait,
        METH_VARARGS,
        "Fire-and-forget async put: 0-copy with ref_count safety\n"
        "Parameters:\n"
        "  path (str): Target file path\n"
        "  buffer (memoryview): Data buffer (read from MemoryObj.byte_array)\n"
        "  size (int): Data size\n"
        "  offset (int): Write offset\n"
        "  memory_obj (MemoryObj): Memory object for ref_count management\n"
        "Returns:\n"
        "  None"
    },
    {
        "GetSteadyClockUs",
        PyWrapper_GetSteadyClockUs,
        METH_NOARGS,
        "Get current steady_clock timestamp in microseconds\n"
        "Returns:\n"
        "  timestamp (int): microseconds since steady_clock epoch"
    },
    {
        NULL, 
        NULL, 
        0, 
        NULL
    }
};

static struct PyModuleDef PyFalconFSInternalModule = {
    PyModuleDef_HEAD_INIT,
    "_pyfalconfs_internal",
    NULL,
    -1,
    PyFalconFSInternalMethods,
    NULL,
    NULL,
    NULL,
    NULL
};

extern "C" PyMODINIT_FUNC PyInit__pyfalconfs_internal(void) 
{
    {
        std::lock_guard guard(AsyncTaskThreadPoolForPyMutex);
        if (!AsyncTaskThreadPoolForPy) {
            const char* envWorkers = std::getenv("FALCON_ASYNC_WORKERS");
            size_t numWorkers = envWorkers ? std::stoul(envWorkers) : 8;
            AsyncTaskThreadPoolForPy = std::make_shared<AsyncTaskThreadPool>(numWorkers);
        }
    }
    PyObject* module = PyModule_Create(&PyFalconFSInternalModule);
    if (PyType_Ready(&AsyncStateType) < 0) 
        return nullptr;
    Py_INCREF(&AsyncStateType);
    PyModule_AddObject(module, "AsyncState", (PyObject*)&AsyncStateType);
    return module;
}