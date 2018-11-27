#ifndef RAX_H
#define RAX_H

#include <stdint.h>

#define RAX_NODE_MAX_SIZE ((1<<29)-1)


typedef struct raxNode {
    uint32_t iskey:1;
    uint32_t isnull:1;
    uint32_t iscompr:1;
    uint32_t size:29;
    
    unsigned char data[];
} raxNode;

typedef struct rax{
    raxNode *head;
    uint64_t numele;
    uint64_t numnodes;
} rax;

#define RAX_STACK_STATIC_ITEMS 32
typedef struct raxStack {
    void **stack;
    size_t items, maxitems;
    void *static_items[RAX_STACK_STATIC_ITEMS];
    int oom;
} raxStack;


#define RAX_ITER_STATIC_LEN 128
#define RAX_ITER_JUST_SEEKED (1<<0)

#define RAX_ITER_EOF (1<<1)
#define RAX_ITER_SAFE (1<<2)

typedef struct raxIterator{
    int flags;
    rax *rt; 
    unsigned char *key;
    void *data;
    size_t key_len;
    size_t key_max;
    unsigned char key_static_string[RAX_ITER_STATIC_LEN];
    raxNode *node;
    raxStack stack;
} raxIterator;


extern void *raxNotFound;
    
rax *raxNew(void);
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old);
void *raxFind(rax *rax, unsigned char *s, size_t len);
void raxFree(rax *rax);
void raxStart(raxIterator *it, rax *rt);
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len);
int raxNext(raxIterator *it);
int raxPrev(raxIterator *it);
int raxRandomWalk(raxIterator *it, size_t steps);
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len);
void raxStop(raxIterator *it);
void raxShow(rax *rax);

#endif
