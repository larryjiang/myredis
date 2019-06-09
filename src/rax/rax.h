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


#define raxNodeFirstChildPtr(n) ((raxNode**)((n)->data + (n)->size + raxPadding((n)->size))) 

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
	if(ts) raxStackPush(ts,h);
	raxNode **children = raxNodeFirstChildPtr(h);
	if(h->iscompr) j = 0;
	memcpy(&h,children+j,sizeof(h));
	parentlink = children + j;
	j = 0;
   }
   
   debugnode("Lookup stop node is",h);	
   if(stopnode) *stopnode = h; 
   if(plink) *plink = parentlink; 
   if(splitpos && h->iscompr) *splitpos = j;
   return i;
};

int raxGenericInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old, int overwrite){
    size_t i;
    int j = 0;
    
    raxNode *h, **parentlink;
    
    debugf("### Insert %.*s with value %p\n",(int)len, s, data);     		
    
    i = raxLowWalk(rax,s,len,&h,&parentlink, &j,NULL); 
    if(i == len  && (!h->iscompr || j == 0)){
        debugf("### Insert: node representing key exists\n");                
        if(!h->iskey || (h->isnull && overwrite)){
            h = raxReallocForData(h,data); 
            if(h) memcpy(parentlink,&h,sizeof(h));
        };
            
        if(h == NULL){
            errno = ENOMEM;
            return 0; 
        };
        
        if(h->iskey){
            if(old) *old = raxGetData(h); 
            if(overwrite) raxSetData(h,data);
            errno = 0;
            return 0;
        
        };
            
        raxSetData(h,data);
        rax->numele++;
        return 1;
    };
        
    if(h->iscompr && i != len){
        debugf("ALGO 1: Stopped at compressed node %.*s (%p) \n",h->size,h->data,(void *)h); 
        debugf("Still to insert: %.*s\n",(int)(len-i), s+i); 
        debugf("Splitting at %d: '%c'\n",j,((char *)h->data)[j]);
        debugf("Ohter (key) letter is '%c'\n",s[i]);
        
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));    
        debugf("Next is %p\n",(void*)next);
        debugf("iskey %d\n",h->iskey);
        if(h->iskey){
            debugf("key value is %p\n",raxGetData(h)); 
        }
        size_t trimmedlen = j;
        size_t postfixlen = h->size - j - 1;
        int split_node_is_key = !trimmedlen && h->iskey && !h->isnull;
        size_t nodesize;

        raxNode *splitnode = raxNewNode(1,split_node_is_key);
        raxNode *trimmed = NULL;
        raxNODE *postfix = NULL;
        
        if(trimmedlen){
            nodesize = sizeof(raxNode)  + trimmedlen + raxPadding(trimmedlen) + sizeof(raxNode*); 
            if(h->iskey && !h->isnull){
                nodesize += sizeof(void *); 
            }
            trimmed = rax_malloc(nodesize);
        }        

        if(postfix){
            nodesize = sizeof(raxNode) + postfixlen + raxPadding(postfixlen) + sizeof(raxNode*);
            postfix = rax_malloc(nodesize); 
        }
        
        if(splitnode == NULL || (trimmedlen && trimmed == NULL) || (postfixlen && postfix == NULL)){
            rax_free(splitnode); 
            rax_free(trimmed);
            rax_free(postfix);
            errno = ENOMEM;
            return 0;
        };
        splitnode->data[0] = h->data[j];
        if(j == 0){
            if(h->iskey){
                void *ndata = raxGetData(h); 
                raxSetData(splitnode,ndata);
            }; 
            memcpy(parentlink,&splitnode,sizeof(splitnode)); 
        }else{
            trimmed->size = j;
            memcpy(trimmed->data,h->data,j); 
            trimmed->iscompr = j > 1 ? 1 : 0;
            trimmed->iskey = h->iskey;
            trimmed->isnull = h->isnull;
            if(h->iskey && !h->isnull){
                void *ndata = raxGetData(h); 
                raxSetData(trimmed,ndata);
            };
            raxNode **cp = raxNodeLastChildPtr(trimmed);
            memcpy(cp,&splitnode,sizeof(splitnode));
            memcpy(parentlink,&trimmed,sizeof(trimmed));
            parentlink = cp;
            postfix->iskey = 0;
            postfix->isnull = 0;
            postfix->size = postfixlen; 
            postfix->iscompr = postfixlen > 1;
            memcpy(postfix->data,h->data+j+1,postfixlen);
            raxNode **cp = raxNodeLastChildPtr(postfix);
            memcpy(cp,&next,sizeof(next));
            rax->numnodes++;  
        }else{
            postfix = next; 
        };
        
        raxNode **splitchild = raxNodeLastChildPtr(splitnode);
        memcpy(splitchild,*postfix,sizeof(postfix));
        rax_free(h);
        h = splitnode;
    }else if(h->iscompr && i == len){
        debugf("ALGO2: Stopped at compressed node %.*s (%p) j = %d\n",h->size,h->data,(void *)h,j); 
        size_t postfixlen = h->size - j;
        size_t nodesize = sizeof(raxNode) + postfixlen + raxPadding(postfixlen) + sizeof(raxNode*);
        if(data != NULL){nodesize += sizeof(void *)};
        raxNode *postfix = rax_malloc(nodesize);
        nodesize = sizeof(raxNode) + j + raxPadding(j) + sizeof(raxNode*);
        
        if(h->iskey && !h->isnull) nodesize += sizeof(void *);
        raxNode *postfix = rax_malloc(nodesize);
        if(postfix == NULL || trimmed == NULL){
            rax_free(postfix); 
            rax_free(trimmed);
            errno = ENOMEM;
            return 0;
        };

        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next, childfield,sizeof(next));

        postfix->size = postfixlen;
        postfix->iscompr = postfixlen > 1;
        postfix->iskey = 1; 
        postfix->isnull = 0;
        memcpy(postfix->data,h->data+j,postfixlen);
        raxSetData(postfix,data);
        raxNode **cp = raxNodeLastChildPtr(postfix);
        memcpy(cp,&next,sizeof(next));
        rax->numnodes++;
        
        trimmed->size = j;
        trimmed->iscompr = j > 1;
        trimmed->iskey = 0;
        trimmed->isnull = 0;
        memcpy(trimmed->data,h->data,j);
        memcpy(parentlink,&trimmed,sizeof(trimmed));
        if(h-iskey){
            void *aux = raxGetData(h); 
            raxSetData(trimmed,aux);
        };
         
        cp = raxNodeLastChildPtr(trimmed);
        memcpy(cp,&postfix,sizeof(postfix));
        rax->numele++;
        rax_free(h);
        return 1;
    };

    while(i < len){
        raxNode * child;
        if(h->size == 0 && len - i > 1){
           debugf("Inserting compressed node \n");  
           size_t comprsize = len - i;
           if(comprsize > RAX_NODE_MAX_SIZE){
                comprsize = RAX_NODE_MAX_SIZE; 
           }
           raxNode *newh = raxCompressNode(h,s+i,comprsize,&child);
           if(newh == NULL) goto oom; 
           h = newh;
           memcpy(parentlink,&h,sizeof(h));
           parentlink = raxNodeLastChildPtr(h);
           i += comprsize;
        }else{
           debugf("Inserting normal node\n"); 
           raxNode **new_parentlink;
           raxNode *newh = raxAddChild(h,s[i],&child,&new_parentlink);        
           if(newh == NULL) goto oom;
           h = newh;
           memcpy(parentlink,&h,sizeof(h));
           parentlink = new_parentlink;
           i++;
        }         
        rax->numnodes++;
        h = child;
    }; 

    raxNode *newh = raxReallocForData(h,data);
    if(newh == NULL) goto oom;
    if(!h->iskey) rax->numele++;
    raxSetData(h,data);
    memcpy(parentlink,&h,sizeof(h));
    return 1;

oom:
    if(h->size == 0){
        h->isnull = 1;
        h->iskey = 1;
        rax->numele++;
        assert(raxRemove(rax,s,i,NULL) != 0); 
    }
    errno = ENOMEM;
    return 0; 
};



int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old){
    return raxGenericInsert(rax,s,len,data,old,1);
};

int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old){
    return raxGenericInsert(rax,s,len,data,old,0);
};

void *raxFind(rax *rax, unsigned char *s, size_t len){
    raxNode *h;
    debugf("### Lookup:%.*s\n",(int)len,s);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,NULL);
    if(i != len || (h->iscompr && splitpos != 0) || !h->iskey){
        return raxNotFound; 
    };
    return raxGetData(h);
};

raxNode **raxFindParentLink(raxNode *parent, raxNode *child){
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode *c;
    while(1){
        memcpy(&c, cp, sizeof(c)); 
        if(c == child) break;
        cp++;
    };
    return cp;
};

raxNode *raxRemoveChild(raxNode *parent, raxNode *child){
    debugnode("raxRemoveChild before", parent);
    if(parent->iscompr){
        void *data = NULL; 
        if(parent->iskey){data = raxGetData(parent);};
        parent->isnull = 0;
        parent->iscompr = 0;
        parent->size = 0;
        if(parent->iskey) raxSetData(parent,data);
        debugnode("raxRemovedChild after",parent);
        return parent;
    };
    
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode **c = cp;
    unsigned char *e = parent->data;

    while(1){
        raxNode *aux;
        memcpy(*aux, c, sizeof(aux)); 
        if(aux == child) break;
        c++;
        e++;
    };

    int taillen = parent->size - (e - parent->data) - 1; 
    debugf("raxRemoveChild tail len: %d\n",taillen);  
    memmove(e,e+1,taillen);
    
    size_t shift = ((parent->size + 4) % sizeof(void *)) == 1 ? sizeof(void *) : 0; 
    if(shift){
        memmove(((char *)cp) - shift, cp, (parent->size - taillen -1) * sizeof(raxNode**)); 
    };

    size_t valuelen = (parent->iskey && !parent->isnull) ? sizeof(void *) : 0;
    memmove(((char *)c)-shift, c+1, taillen * sizeof(raxNode**) + valuelen);

    parent->size--;
    
    raxNode *newnode = rax_realloc(parent,raxNodeCurrentLength(parent));
    if(newnode){
        debugnode("raxRemoveChild after", newnode); 
    };
    return newnode ? newnode : parent;
}


int raxRemove(rax *rax, unsigned char *s, size_t len, void **old){
    raxNode *h;
    raxStack ts;

    debugf("### Delete: %.*s\n",(int)len,s);
    raxStackInit(&ts);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,&ts);
    if(i != len || (h->iscompr && splitpos != 0) || !h->iskey){
        raxStackFree(&ts); 
        return 0;
    };
    if(old) *old = raxGetData(h);
    h->iskey = 0;
    rax->numele--;
        
    int trycompress = 0;
    
    if(h->size == 0){
        debugf("Key deleted in node without children. Cleanup needed.\n");      
        raxNode *child = NULL;
        while(h != rax->head){
           child = h;  
           debugf("Freeing child %p [%.*s] key:%d\n",(void *)child, (int)child->size,(char *)child->data,child->iskey);
           rax_free(child);
           rax->numnodes--;
           h = raxStackPop(&ts); 
           if(h->iskey || (!h->iscompr && h->size != 1)) break;
        };
        if(child){
            debugf("Unlinking child %p from parent %p\n",(void *)child, (void *)h); 
            raxNode *new = raxRemoveChild(h,child);
            if(new != h){
                raxNode *parent = raxStackPeek(&ts);  
                raxNode **parentlink;
                if(parent == NULL){
                    parentlink = &rax->head; 
                }else{
                    parentlink = raxFindParentLink(parent,h); 
                }
                memcpy(parentlink,&new, sizeof(new)); 
            };
            if(new->size == 1 && new->iskey==0){
                trycompress = 1;
                h = new; 
            }; 
        };
    }else if(h->size == 1){
        trycompress = 1;     
    } 

    if(trycompress && ts.oom) trycompress = 0;

    

};


