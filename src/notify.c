#include "server.h"

int keyspaceEventsStringToFlags(char *classes){
    char *p = classes;
    int c, flags = 0;

    while((c = *p++) != '\0'){
        switch (c)
        {
        case 'A':
            flags |= NOTIFY_ALL;
            break;
        case 'g':
            flags |= NOTIFY_GENERIC;
            break;
        case '$':
            flags |= NOTIFY_STRING;
            break;
        case 'l':
            flags |= NOTIFY_LIST;
            break;
        case 's':
            flags |= NOTIFY_SET;
            break;
        case 'h':
            flags |= NOTIFY_HASH;
            break;
        case 'z':
            flags |= NOTIFY_ZSET;
            break;
        case 'x':
            flags |= NOTIFY_EXPIRED;
            break;
        case 'e':
            flags |= NOTIFY_EVICTED;
            break;
        case 'K':
            flags |= NOTIFY_KEYSPACE;
            break;
        case 'E':
            flags |= NOTIFY_KEYEVENT;
            break;
        default:
            return -1;
        }
    };
    return flags;
};

sds keyspaceEventsFlagsToString(int flags){
    sds res;

    res = sdsempty();
    if((flags & NOTIFY_ALL) == NOTIFY_ALL){
        res = sdscatlen(res,"A",1);
    }else{
        if (flags & NOTIFY_GENERIC) res = sdscatlen(res,"g",1);
        if (flags & NOTIFY_STRING) res = sdscatlen(res,"$",1);
        if (flags & NOTIFY_LIST) res = sdscatlen(res,"l",1);
        if (flags & NOTIFY_SET) res = sdscatlen(res,"s",1);
        if (flags & NOTIFY_HASH) res = sdscatlen(res,"h",1);
        if (flags & NOTIFY_ZSET) res = sdscatlen(res,"z",1);
        if (flags & NOTIFY_EXPIRED) res = sdscatlen(res,"x",1);
        if (flags & NOTIFY_EVICTED) res = sdscatlen(res,"e",1);
    }

    if (flags & NOTIFY_KEYSPACE) res = sdscatlen(res,"K",1);
    if (flags & NOTIFY_KEYEVENT) res = sdscatlen(res,"E",1);
    return res;
};

void notifyKeyspaceEvent(int type, char *event, robj *key, int dbid){
    sds chan;
    robj *chanobj, *eventobj;
    int len = -1;
    char buf[24];

    if(!(server.notify_keyspace_events & type)) return;

    if(server.notify_keyspace_events & NOTIFY_KEYSPACE){
        chan = sdsnewlen("__keyspace@",11);
        len = ll2string(buf,sizeof(buf),dbid);
        chan = sdscatlen(chan,buf,len);
        chan = sdscatlen(chan,"__:",3);
        chan = sdscatsds(chan,key->ptr);
        chanobj = createObject(OBJ_STRING,chan);   
        pubsubPublishMessage(chanobj,eventobj);
        decrRefCount(chanobj);
    };

    if(server.notify_keyspace_events & NOTIFY_KEYEVENT){
        chan = sdsnewlen("__keyevent@",11);
        if(len == -1) len = ll2string(buf,sizeof(buf),dbid);
        chan = sdscatlen(chan,buf,len);
        chan = sdscatlen(chan,"__:",3);
        chan = sdscatsds(chan,eventobj->ptr);
        pubsubPublishMessage(chanobj,key);
        decrRefCount(chanobj);
    };
    decrRefCount(eventobj);
};




