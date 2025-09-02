#ifndef FALCONFS_KV_META_H
#define FALCONFS_KV_META_H

#include <stdint.h>

int FalconKvInit(std::string &path);
int FalconKvShmAndIpcServiceInit(void);
int FalconKvPutData(const std::string &key, const void* vaule, const size_t len);
int FalconKvGetData(const std::string &key, void* vaule);
int FalconKvDeleteKey(const std::string &key);


#endif // FALCONFS_KV_META_H
