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
#include "kv_meta.h"


/*  =============================KV Cache=============================  */

static void KvInit(const char* workspace, const char* runningConfigFile)
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
    setenv("WORKSPACE_PATH", workspace, 1);

    int ret = -1;
    ret = GetInit().Init();
    if (ret != FALCON_SUCCESS)
        throw std::runtime_error("Falcon init failed. Error: " + std::to_string(ret));

    std::string socketPath = std::string(workspace) + "/falconfs_kv_socket.s";

    ret = FalconKvInit(socketPath);
    if (ret != FALCON_SUCCESS)
        throw std::runtime_error("Falcon cluster failed." + std::to_string(ret));
}

static PyObject* PyWrapper_KvInit(PyObject* self, PyObject* args)
{
    char* workspace = nullptr;
    char* runningConfigFile = nullptr;
    if (!PyArg_ParseTuple(args, "ss", &workspace, &runningConfigFile))
        return NULL;

    try
    {
        KvInit(workspace, runningConfigFile);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    Py_RETURN_NONE;
}

static int KvPut(const char* key, Py_buffer buffer)
{
    return FalconKvPutData(key, (void*)buffer.buf, buffer.len);
}

static PyObject* PyWrapper_KvPut(PyObject* self, PyObject* args)
{
    char *key = nullptr;
    Py_buffer value;
    if (!PyArg_ParseTuple(args, "sw*", &key, &value))
        return NULL;

    int ret = -1;
    try
    {
        ret = KvPut(key, value);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    return PyLong_FromLong(ret);
}

static int KvGet(const char* key, Py_buffer buffer)
{
    return FalconKvGetData(key, (void*)buffer.buf);
}

static PyObject* PyWrapper_KvGet(PyObject* self, PyObject* args)
{
    char *key = nullptr;
    Py_buffer value;
    if (!PyArg_ParseTuple(args, "sw*", &key, &value))
        return NULL;

    int ret = -1;
    try
    {
        ret = KvGet(key, value);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    return PyLong_FromLong(ret);
}

static int KvDelete(const char* key)
{
    return FalconKvDeleteKey(key);
}

static PyObject* PyWrapper_KvDelete(PyObject* self, PyObject* args)
{
    char *key = nullptr;
    if (!PyArg_ParseTuple(args, "s", &key))
        return NULL;

    int ret = -1;
    try
    {
        ret = KvDelete(key);
    }
    catch (const std::exception& e)
    {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }

    return PyLong_FromLong(ret);
}

static PyMethodDef PyFalconFSInternalMethods[] =
{
    {
        "KvInit",
        PyWrapper_KvInit,
        METH_VARARGS,
        "Initialize KV FalconFS with workspace and running config file\n"
        "Parameters:\n"
        "  workspace (str): Workspace for python falconfs client\n"
        "  running_config_file (str): Configuration file path of running fuse-based falconfs\n"
        "Returns:\n"
        "  NONE"
    },
    {
        "KvPut",
        PyWrapper_KvPut,
        METH_VARARGS,
        "Put KvCache data in FalconFS\n"
        "Parameters:\n"
        "  key (str): KvCache key\n"
        "  value (tensor): KvCache value\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "KvDelete",
        PyWrapper_KvDelete,
        METH_VARARGS,
        "Delete KvCache data in FalconFS\n"
        "Parameters:\n"
        "  key (str): KvCache key\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
    },
    {
        "KvGet",
        PyWrapper_KvGet,
        METH_VARARGS,
        "Get KvCache data from FalconFS\n"
        "Parameters:\n"
        "  key (str): KvCache key\n"
        "  value (tensor): KvCache value\n"
        "Returns:\n"
        "  errno (int): Refer to errno in linux"
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