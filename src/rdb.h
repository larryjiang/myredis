#ifndef __RDB_H
#define __RDB_H

#include <stdio.h>
#include "rio.h"


#include "server.h"
#define RDB_VERSION 8

#define RDB_6BITLEN 0
#define RDB_14BITLEN 1
#define RDB_32BITLEN 0x80
#define RDB_64BITLEN 0x81
#define RDB_ENCVAL 3
#define RDB_LENERR UINT64_MAX


#define RDB_ENC_INT8 0
#define RDB_ENC_INT16 1
#define RDB_ENC_INT32 2
#define RDB_ENC_LZF 3

#define RDB_TYPE_STRING 0
#define RDB_TYPE_LIST 1
#define RDB_TYPE_SET 2
#define RDB_TYPE_ZSET 3
#define RDB_TYPE_HASH 4
#define RDB_TYPE_ZSET_2 5
#define RDB_TYPE_MODULE 6





#define RDB_TYPE_HASH_ZIPMAP 9
#define RDB_TYPE_LIST_ZIPLIST 10
#define RDB_TYPE_SET_INTSET 11
#define RDB_TYPE_ZSET_ZIPLIST 12
#define RDB_TYPE_HASH_ZIPLIST 13
#define RDB_TYPE_LIST_QUICKLIST 14

#define rdbIsObjectType(t) ((t >= 0 && t<= 6) || (t >= 9 && t <= 14))

#define RDB_OPCODE_AUX 250
#define RDB_OPCODE_RESIZEDB 251
#define RDB_OPCODE_EXPIRETIME_MS 252
#define RDB_OPCODE_EXPIRETIME 253
#define RDB_OPCODE_SELECTDB 254
#define RDB_OPCODE_EOF 255

#define RDB_LOAD_NONE 0
#define RDB_LOAD_ENC (1<<0)
#define RDB_LOAD_PLAIN (1<<1)
#define RDB_LOAD_SDS (1<<2)

#define RDB_SAVE_NONE 0

#define RDB_SAVE_AOF_PREAMBLE (1<<0)

int rdbSaveType(rio *rdb, unsigned char type);
int rdbLoadType(rio *rdb);
int rdbSaveTime(rio *rdb, time_t t);
time_t rdbLoadTime(rio *rdb);
int rdbSaveLen(rio *rdb, uint64_t len);
uint64_t rdbLoadLen(rio *rdb, int *isencoded);
int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr);
int rdbSaveObjectType(rio *rdb, robj *o);
int rdbLoadObjectType(rio *rdb);
int rdbLoad(char *filename, rdbSaveInfo *rsi);
int rdbSaveBackground(char *filename, rdbSaveInfo *rsi);
int rdbSaveToSlavesSockets(rdbSaveInfo *rsi);
void rdbRemoveTempFile(pid_t childpid);
int rdbSave(char *filename, rdbSaveInfo *rsi);
ssize_t rdbSaveObject(rio *rdb, robj *o);
size_t rdbSavedObjectLen(robj *o);
robj *rdbLoadObject(int type, rio *rdb);
void backgroundSaveDoneHandler(int exitcode, int bysignal);
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, long long expiretime, long long now);
robj *rdbLoadStringObject(rio *rdb);
int rdbSaveStringObject(rio *rdb, robj *obj);
ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len);
void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr);
int rdbSaveBinaryDoubleValue(rio *rdb, double val);
int rdbLoadBinaryDoubleValue(rio *rdb, double *val);
int rdbSaveBinaryFloatValue(rio *rdb, float val);
int rdbLoadBinaryFloatValue(rio *rdb, float *val);
int rdbLoadRio(rio *rdb, rdbSaveInfo *rsi);

#endif
