#ifndef LMY_FALCON_CONTROL_META_EXPIRATION_H
#define LMY_FALCON_CONTROL_META_EXPIRATION_H

#include "postgres.h"

void FalconDaemonMetaExpirationProcessMain(Datum main_arg);

#define FALCON_META_NEVER_EXPIRE ((int32_t)(-1))
extern int32_t FalconMetaValidDuration;
bool ExceedExpirationTimeInterval(int64_t accessTime, int64_t currentTime);
bool NeedRenewMetaAccessTime(int64_t accessTime, int64_t currentTime);

#endif