#ifndef FALCONFS_KV_DLOPEN_INIT_H
#define FALCONFS_KV_DLOPEN_INIT_H

#include <cstdio>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include "hcom/kv_hcom_service.h"

/*******************************************************************************
  Function Name:  InitKvHcomIpcDl
  Function Usage:  dlopen libhcom.so, reg function of hcom shm
  Input Parameter:  the path of libhcom.so
  Output Parameter:  None
  Return:  0 for success and -1 for failed
*******************************************************************************/
int InitKvHcomIpcDl(std::string &dlPath, uint32_t pathLen);

/*******************************************************************************
  Function Name:  FinishKvHcomIpcDl
  Function Usage:  doesnot use libhcom.so again. dlclose it
  Input Parameter:  None
  Output Parameter:  None
  Return:  None
*******************************************************************************/
void FinishKvHcomIpcDl(void);

#endif  // FALCONFS_KV_DLOPEN_INIT_H
