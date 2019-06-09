#include "server.h"
#include "slowlog.h"



slowlogEntry *slowlogCreateEntry(robj **argv, int argc, long long duration){
    slowlogEntry *se = zmalloc(sizeof(*se));
    int j, slargc = argc;

    if(slargc > SLOWLOG_ENTRY_MAX_ARGC) slargc = SLOWLOG_ENTRY_MAX_ARGC;
    se->argc = slargc;
    se->argv = zmalloc(sizeof(robj*) * slargc);


    for(j = 0; j < slargc; j++){
        if(slargc != argc && j == slargc - 1){
            se->argv[j] = createObject(OBJ_STRING, sdscatprintf(sdsempty,"...(%d more arguments)",argc-slargc + 1)); 
        }else{
            if(argv[j]->type == OBJ_STRING &&
               sdsEncodedObject(argv[j]) &&
               sdslen(argv[j]->ptr) > SLOWLOG_ENTRY_MAX_STRING){
                sds s = sdsnewlen(argv[j]->ptr, SLOWLOG_ENTRY_MAX_STRING);
                s = sdscatprintf(s,"... (%u more bytes)", (unsigned long) sdslen(argv[j]->ptr) - SLOWLOG_ENTRY_MAX_STRING);
                se->argv[j] = createObject(OBJ_STRING,s);
            }else{
                se->argv[j] = argv[j]; 
                incrRefCount(argv[j]);
            } 
        } 
    };
    se->time = time(NULL);
    se->duration = duration;
    se->id = server.slowlog_entry_id++;
    return se;
};

void slowlogFreeEntry(void *septr){
    slowlogEntry *se = septr;
    int j;

    for(j = 0; j < se->argc; j++){
        decrRefCount(se->argv[j]); 
    };

    zfree(se->argv);
    zfree(se);
};


void slowlogInit(void){
    server.slowlog = listCreate();
    server.slowlog_entry_id = 0;
    listSetFreeMethod(server.slowlog,slowlogFreeEntry);
};

void slowlogPushEntryIfNeeded(robj **argv, int argc, long long duration){
    if(server.slowlog_log_slower_than < 0) return;
    if(duration >= server.slowlog_log_slower_than){
        listAddNodeHead(server.slowlog, slowlogCreateEntry(argv,argc,duration)); 
    };
    while(listLength(server.slowlog) > server.slowlog_max_len){
        listDelNode(server.slowlog,listLast(server.slowlog)); 
    };
};


void slowlogReset(void){
    while(listLength(server.slowlog) > 0){
        listDelNode(server.slowlog, listLast(server.slowlog)); 
    };
};

void slowlogCommand(client *c){
    if(c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "reset")){
        slowlogReset(); 
        addReply(c,shared.ok);
    }else if(c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "len")){
        addReplyLongLong(c,listLength(server.slowlog)); 
    }else if((c->argc == 2 || c->argc == 3) && 
             !strcasecmp(c->argv[1]->ptr,"get") 
            ){
            long count = 10, sent = 0; 
            listIter li;
            void *totentries;
            listNode *ln;
            slowlogEntry *se;

            if(c->argc == 3 && getLongFromObjectOrReply(c,c->argv[2],&count,NULL)!= C_OK){
                return; 
            }
            
           listRewind(server.slowlog,&li); 
           totentries = addDeferredMultiBulkLength(c);
           while(count-- && (ln = listNext(&li))){
                int j;
                se = ln->value; 
                addReplyMultiBulkLen(c,4);
                addReplyLongLong(c,se->id);
                addReplyLongLong(c,se->time);
                addReplyLongLong(c,se->duration);
                addReplyMultiBulkLen(c,se->argc);
                for(j = 0; j < se->argc; j++){
                    addReplyBulk(c,se->argv[j]); 
                }
                sent++;
           };
           setDeferredMultiBulkLength(c,totentries,sent);
    }else{
        addReplyError(c,"Unknown SLOWLOG subcommand or wrong # of args, Try GET, RESET, LEN."); 
    };
};

