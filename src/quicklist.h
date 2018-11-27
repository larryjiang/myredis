#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__


typedef struct quicklistNode {
    struct quicklistNode *prev;
    struct quicklistNode *next;
    
    unsigned char *zl;
    unsigned int sz;
    unsigned int count : 16;
    unsigned int encoding : 2;
    unsigned int container : 2;
    unsigned int recompress : 1;
    unsigned int attempted_compress : 1;
    unsigned int extra : 10;
} quicklistNode;


typedef struct quicklistLZF{
    unsigned int sz;
    char compressed[];
} quicklistLZF;


typedef struct quicklist {
    quicklistNode *head;
    quicklistNode *tail;
    unsigned long count;
    unsigned int len;
    int fill : 16;
    unsigned int compress : 16;
} quicklist;


typedef struct quicklistIter {
    const quicklist *quicklist;
    quicklistNode *current;
    unsigned char *zi;
    long offset;
    int direction;
} quicklistIter;


typedef struct quicklistEntry{
    const quicklist *quicklist;
    quicklistNode *node;
    unsigned char *zi;
    unsigned char *value;
    long long longval;
    unsigned int sz;
    int offset;
} quicklistEntry;


#define QUICKLIST_HEAD 0
#define QUICKLIST_TAIL -1

#define QUICKLIST_NODE_ENCODING_RAW 1
#define QUICKLIST_NODE_ENCODING_LZF 2

#define QUICKLIST_NOCOMPRESS 0

#define QUICKLIST_NODE_CONTAINER_NONE 1
#define QUICKLIST_NODE_CONTAINER_ZIPLIST 2

#define quicklistNodeIsCompressed(node) ((node) >encoding == QUICKLIST_NODE_ENCODING_LZF) 

quicklist *quicklistCreate(void);
quicklist *quicklistNew(int fill, int compress);
void quicklistSetCompressDepth(quicklist *quicklist, int depth);
void quicklistSetFill(quicklist *quicklist, int fill);
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);
void quicklistRelease(quicklist *quicklist);
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
void quicklistPush(quicklist *quicklist, void *value, const size_t sz, int where);
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl);
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist, unsigned char *zl);
quicklist *quicklistCreateFromZiplist(int fill, int compress, unsigned char *zl);

void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *node, void *value, const size_t sz);
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *node, void *value, const size_t sz);

void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data, int sz);

int quicklistDelRange(quicklist *quicklsit, const long start, const long stop);


quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction);
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist, int direction, const long long idx);


int quicklistNext(quicklistIter *iter, quicklistEntry *node);
void quicklistReleaseIterator(quicklistIter *iter);
quicklist *quicklistDup(quicklist *orig);
int quicklistIndex(const quicklist *quicklist, const long long index, quicklistEntry *entry);

void quicklistRewind(quicklist *quicklist, quicklistIter *li);
void quicklistRewindTail(quicklist *quicklist, quicklistIter *li);

void quicklistRotate(quicklist *quicklist);
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data, unsigned int *sz, long long *sval, void *(*saver)(unsigned char *data,unsigned int sz));

int quicklistPop(quicklist *quicklist, int where, unsigned char **data, unsigned int *sz, long long *slong);
unsigned int quicklistCount(const quicklist *ql);
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len);
size_t quicklistGetLzf(const quicklistNode *node, void **data);


#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[]);
#endif


#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif



