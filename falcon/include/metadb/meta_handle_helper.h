#ifndef FALCON_METADB_META_HANDLE_HELPER_H
#define FALCON_METADB_META_HANDLE_HELPER_H

#include "postgres.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "utils/relcache.h"

Oid GetRelationOidByName_FALCON(const char *relationName);

enum ModeCheckType
{
    MODE_CHECK_NONE,
    MODE_CHECK_MUST_BE_FILE,
    MODE_CHECK_MUST_BE_DIRECTORY
};
bool SearchAndUpdateInodeTableInfo(const char *workerInodeRelationName,
                                   Relation workerInodeRelation,
                                   const char *workerInodeRelationIndexName,
                                   Oid workerInodeIndexOid,
                                   const uint64_t parentId_partId,
                                   const char *fileName,
                                   const bool doUpdate,
                                   uint64_t *inodeId,
                                   int64_t *size,
                                   const int64_t *newSize,
                                   uint64_t *updateVersion,
                                   uint64_t *nlink,
                                   const int nlinkChangeNum,
                                   mode_t *mode,
                                   const mode_t *newExecMode,
                                   int modeCheckType,
                                   uint32_t *newUid,
                                   uint32_t *newGid,
                                   const char *newEtag,
                                   TimestampTz *newAtime,
                                   TimestampTz *newMtime,
                                   int32_t *primaryNodeId,
                                   int32_t *newPrimaryNodeId,
                                   int32_t *backupNodeId);

uint16_t HashPartId(const char *fileName);
uint64_t CombineParentIdWithPartId(uint64_t parent_id, uint16_t part_id);

StringInfo GetInodeShardName(int shardId);
StringInfo GetInodeIndexShardName(int shardId);
StringInfo GetXattrShardName(int shardId);
StringInfo GetXattrIndexShardName(int shardId);

StringInfo GetSliceShardName(int shardId);
StringInfo GetSliceIndexShardName(int shardId);
StringInfo GetKvmetaShardName(int shardId);
StringInfo GetKvmetaIndexShardName(int shardId);

#endif