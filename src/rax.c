#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include "rax.h"


#ifndef RAX_MALLOC_INCLUDE
#define RAX_MALLOC_INCLUDE "rax_malloc.h"
#endif

#include RAX_MALLOC_INCLUDE


void *raxNotFound = (void *)"rax-not-found-pointer";

void raxDebugShowNode(const char *msg, raxNode *n);

#if 0
#define debugf(...)    \
    do{   \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__,__LINE__);  \
        printf(__VA_ARGS__);  \
        fflush(stdout);  \
    }while(0);        

#define debugnode(msg,n) raxDebugShowNode(msg,n)
#else
#define debugf(...)
#define debugnode(msg,n)
#endif


static inline void raxStackInit(raxStack *ts){
    ts->stack = ts->static_items;
    ts->items = 0;
    ts->maxitems = RAX_STACK_STATIC_ITEMS;
    ts->oom = 0;
};




