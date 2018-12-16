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

static inline int raxStackPush(raxStack *ts, void *ptr){
    if(ts->items == ts->maxitems){
        if(ts->stack == ts->static_items){
            ts->stack = rax_malloc(sizeof(void *) * ts->maxitems * 2);
            if(ts->stack == NULL){
                ts->stack = ts->static_items;
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            
            memcpy(ts->stack, ts->static_items, sizeof(void*) * ts->maxitems);

        }else{
            void **newalloc = rax_realloc(ts->stack, sizeof(void *) * ts->maxitems * 2);
            if(newalloc == NULL){
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            ts->stack = newalloc;
        }        
        ts->maxitems *= 2;
    }
    ts->stack[ts->items] = ptr;
    ts->items++;
    return 1;
};

static inline void *raxStackPop(raxStack *ts){
    if(ts->items == 0) return NULL;
    ts-items--;
    return ts->stack[ts->items];
};

static inline void *raxStackPeek(raxStack *ts){
    if(ts->items == 0) return NULL;
    return ts->stack[ts->items - 1];
}

static inline void raxStackFree(raxStack *ts){
    if(ts->stack != ts->static_items) rax_free(ts->stack);
}

raxNode *raxNewNode(size_t children, int datafield){
    size_t nodesize = sizeof(raxNode) + children + sizeof(raxNode *) * children; 
    if(datafield) nodesize += sizeof(void*);
    raxNode *node = rax_malloc(nodesize); 
    if(node == NULL) return NULL;   
    node->iskey = 0;
    node->isnull = 0;
    node->iscompr = 0;
    node->size = children;
    return node;
}

rax *raxNew(void){
    rax *rax = rax_malloc(sizeof(*rax));
    if(rax == NULL) return NULL;
    rax->numele = 0;
    rax->numnodes = 1;
    rax->head = raxNewNode(0,0);
    if(rax->head == NULL){
        rax_free(rax);
        return NULL;
    }else{
        return rax;
    }
}

#define raxNodeCurrentLength(n) ( \
    sizeof(raxNode) + (n)->size + \
    ((n)->iscompr ? sizeof(raxNode *) : sizeof(raxNode *) *(n)->size) + \
    (((n)->iskey && !(n)->isnull) * sizeof(void *)) \
    )

raxNode *raxReallocForData(raxNode *n, void *data){
    if(data == NULL) return n;
    size_t curlen = raxNodeCurrentLength(n); 
    return rax_realloc(n, curlen+sizeof(void *));
};

void raxSetData(raxNode *n, void *data){
    n->iskey = 1;
    if(data != NULL){
        n->isnull = 0;
        void **ndata = (void **) ((char *)n + raxNodeCurrentLength(n) - sizeof(void *));
        memcpy(ndata, &data,sizeof(data));
    }else{
        n->isnull = 1;
    };
};



void *raxGetData(raxNode * n){
    if(n->isnull) return NULL;
    void **ndata = (void**)((char *)n + raxNodeCurrentLength(n) - sizeof(void *));
    void *data;
    memcpy(&data,ndata,sizeof(data));
    return data;
};


raxNode *raxAddChild(raxNode *n, unsigned char c, raxNode **childptr, raxNode ***parentlink){
    assert(n->iscompr == 0);
    
    size_t curlen = sizeof(raxNode) + n->size + sizeof(raxNode*)*n->size;
    size_t newlen;
    
    raxNode *child = raxNewNode(0,0);
    if(child == NULL) return NULL;
    
    if(n->iskey) curlen += sizeof(void *);  
    newlen = curlen + sizeof(raxNode *) + 1;
    raxNode *newn = rax_realloc(n,newlen);
    if(newn == NULL){
        rax_free(child);
        return NULL;
    }
    n = newn;
    
    int pos;
    for(pos = 0; pos < n->size;pos++){
        if(n->data[pos] > c){
            break;
        };
    } 
    
    unsigned char *src;
    
    if(n->iskey && !n->isnull){
        src = n->data + n->size + sizeof(raxNode*) * n->size;
        memmove(src + 1 + sizeof(raxNode*),src,sizeof(void *));
    };
    
    src = n->data + n->size + sizeof(raxNode*)*pos;
    memmove(src + 1 + sizeof(raxNode*), src, sizeof(raxNode*)*(n->size - pos)); 
    
    src = n->data + pos;
    memmove(src + 1, src, n->size - pos + sizeof(raxNode*)*pos); 
    
    n->data[pos] = c; 
    n->size++;
    
    raxNode **childfield = (raxNode**)(n->data + n->size + sizeof(raxNode*)*pos);    
    memcpy(childfield,&child,sizeof(child));
    *childptr = child;
    *parentlink = childfield;
    return n;
};


#define raxNodeLastChildPtr(n) ((raxNode**) (   \
      ((char *)(n)) + \
      raxNodeCurrentLength(n) - \
      sizeof(raxNode*) - \
      (((n)->iskey && !(n)->isnull) ? sizeof(void *) : 0)  \
))


#define raxNodeFirstChildPtr(n) ((raxNode**)((n)->data + (n)->size)) 

raxNode *raxCompressNode(raxNode *n, unsigned char *s, size_t len, raxNode **child){
    assert(n->size == 0 && n->iscompr == 0);
    
    void *data = NULL;
    
    size_t newsize;
    debugf("Compress node: %.*s\n",(int)len,s);
    
    *child = raxNewNode(0,0);
    if(*child == NULL){return NULL};
    
    newsize = sizeof(raxNode) + len + sizeof(raxNode*);
    if(n->iskey){
        data = raxGetData(n);
        if(!n->isnull) newsize += sizeof(void *);
    };
    
    raxNode *newn = rax_realloc(n,newsize);
    if(newn == NULL){
        rax_free(*child);
        return NULL;
    };
    n = newn;
    n->iscompr = 1;
    n->size = len;
    memcpy(n->data,s,len);
    if(n->iskey)raxSetData(n,data);
    raxNode **childfield = raxNodeLastChildPtr(n);
    memcpy(childfield,child,sizeof(*child));
    return n;
};


static inline size_t raxLowWalk(rax *rax, unsigned char *s, size_t len, raxNode **stopnode, raxNode ***plink, int *splitpos, raxStack *ts){
    raxNode *h = rax->head;
    raxNode **parentlink = &rax->head;
    
    size_t i = 0;
    size_t j = 0;
    
    while(h->size && i < len){
        debugnode("Lookup current node",h);
        unsigned char *v = h->data;
        
        if(h->iscompr){
            for(j = 0; j < h->size && i < len; j++, i++){
                if(v[j] != s[i]) break;
            };
            if(j != h->size) break;
        }else{
            for(j = 0; j < h->size; j++){
                if(v[j] == s[i])break;
            };
            if(j == h->size)break;
            i++;
        };







};



