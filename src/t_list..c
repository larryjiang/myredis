#include "server.h" 

void listTypePush(robj *subject, robj *value, int where){
    if(subject->encoding == OBJ_ENCODING_QUICKLIST){
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        value = getDecodedObject(value);
        size_t len = sdslen(value->ptr);
        quicklistPush(subject->ptr, value->ptr,len,pos);
        decrRefCount(value);
    }else{
        serverPanic("Unknown list encoding");
    }
};

void *listPopSaver(unsigned char *data, unsigned int sz){
    return createStringObject((char *)data,  sz);
};

robj *listTypePop(robj *subject, int where){
    long long vlong;
    robj *value = NULL;

    int ql_where = where = LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if(subject->encoding == OBJ_ENCODING_QUICKLIST){
        if(quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,NULL,&vlong, listPopSaver)){
            if(!value){
                value = createStringObjectFromLongLong(vlong);
            };
        };
    }else{
        serverPanic("Unknown list encoding");
    }
    return value;
};

unsigned long listTypeLength(const robj *subject){
    if(subject->encoding == OBJ_ENCODING_QUICKLIST){
        return quicklistCount(subject->ptr);
    }else{
        serverPanic("UNknown list encoding");
    }
};


void unblockClientWaitingData(client *c){
    dictEntry *de;
    dictIterator *di;
    list *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);

    di = dictGetIterator(c->bpop.keys);

    while((de = dictNext(di)) != NULL){
        robj *key = dictGetKey(de);

        l = dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        listDelNode(l,listSearchKey(l,c));
        if(listLength(l) == 0){
            dictDelete(c->db->blocking_keys,key);
        }
    };
    dictReleaseIterator(di);

    dictEmpty(c->bpop.keys,NULL);
    if(c->bpop.target){
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    };
};












