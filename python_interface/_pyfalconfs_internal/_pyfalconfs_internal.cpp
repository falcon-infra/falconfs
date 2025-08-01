#pragma GCC diagnostic ignored "-Wmissing-field-initializers"

#include <Python.h>

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
    errno_t err = memset_s(&st, sizeof(st), 0, sizeof(st));
    if (err != 0) {
        return -err;
    }
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
    errno_t err = memset_s(&st, sizeof(st), 0, sizeof(st));
    if (err != 0) {
        return -err;
    }
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
        errno_t err = memmove_s((char *)(lastSlash + 1),
                                strlen(lastSlash + 1) + 1,
                                (char *)(lastSlash + 3),
                                strlen(lastSlash + 3) + 1);
        if (err != 0) 
        {
            return -err;
        }
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
    
    return Py_BuildValue("(iN)", ret, dict);
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

static int Exists(const char* path)
{
    int ret = -1;
    struct stat stbuf;
    ret = Stat(path, &stbuf);
    if (ret != 0)
        return ret;

    return ret;
}
static PyObject* PyWrapper_Exists(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    if (!PyArg_ParseTuple(args, "s", &path)) 
        return nullptr;

    int ret = Exists(path);
    
    return PyLong_FromLong(ret);
}

static int Get(const char* path, Py_buffer buffer, int size, int offset)
{
    int ret = -1;
    int readSize;
    uint64_t fd = UINT64_MAX;

    ret = Open(path, O_RDONLY, fd);
    if (ret != 0)
        return ret;
    
    readSize = Read(path, fd, (char*)buffer.buf, size, offset);
    if (readSize < 0)
    {
        Close(path, fd);
        return ret;
    }
            
    ret = Close(path, fd);
    if (ret != 0)
        return ret;

    return ret;
}
static PyObject* PyWrapper_Get(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sw*ii", &path, &buffer, &size, &offset)) 
        return nullptr;

    int ret = Get(path, buffer, size, offset);

    return PyLong_FromLong(ret);
}

static int Put(const char* path, Py_buffer buffer, int size, int offset)
{
    int ret = -1;
    uint64_t fd = UINT64_MAX;

    ret = Create(path, O_WRONLY, fd);
    if (ret != 0 && ret != -EEXIST)
        return ret;

    ret = Write(path, fd, (char*)buffer.buf, size, offset);
    if (ret != 0)
    {
        Close(path, fd);
        return ret;
    }

    ret = Flush(path, fd);
    if (ret != 0)
    {
        Close(path, fd);
        return ret;
    }
        
    ret = Close(path, fd);
    if (ret != 0)
        return ret;

    return ret;
}
static PyObject* PyWrapper_Put(PyObject* self, PyObject* args) 
{
    char* path = nullptr;
    Py_buffer buffer;
    int size;
    int offset;
    if (!PyArg_ParseTuple(args, "sw*ii", &path, &buffer, &size, &offset)) 
        return nullptr;

    int ret = Put(path, buffer, size, offset);

    return PyLong_FromLong(ret);
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
        "Write data to file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file/directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
        "  stbuf (dict): Info of target"
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
        "Exists", 
        PyWrapper_Exists, 
        METH_VARARGS, 
        "Check file/directory exists in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file/directory path, must start with '/', which corresponding to mount point\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
    },
    {
        "Get", 
        PyWrapper_Get, 
        METH_VARARGS, 
        "Get file content in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  buffer (bytearray): Space to store data\n"
        "  size (int): Requested size\n"
        "  offset (int): Read offset\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
    },
    {
        "Put", 
        PyWrapper_Put, 
        METH_VARARGS, 
        "Put data to file in FalconFS\n"
        "Parameters:\n"
        "  path (str): Target file path, must start with '/', which corresponding to mount point\n"
        "  buffer (bytearray): Space of stored data\n"
        "  size (int): To write size\n"
        "  offset (int): Write offset\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux\n"
    },
    {
        NULL, 
        NULL, 
        0, 
        NULL
    }
};

static int ModuleCleanup(PyObject* module) 
{
    return 0;
}

static struct PyModuleDef PyFalconFSInternalModule = {
    PyModuleDef_HEAD_INIT,
    "_pyfalconfs_internal",
    NULL,
    -1,
    PyFalconFSInternalMethods,
    NULL,
    NULL,
    ModuleCleanup,
    NULL
};

extern "C" PyMODINIT_FUNC PyInit__pyfalconfs_internal(void) 
{
    PyObject* module = PyModule_Create(&PyFalconFSInternalModule);

    return module;
}