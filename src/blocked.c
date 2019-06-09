#include "server.h"


void unblockClient(client *c){
    if(c->btype == BLOCKED_LIST){
        unblockClientWaitingData(c);
    }else if(c->btype == BLOCKED_WAIT){
       unblockClientWaitingReplicas(c); 
    }else if(c->btype == BLOCKED_MODULE){
        unblockClientFromModule(c);
    }else{
        serverPanic("Unknown btype in unblockClient()");
    }

    c->flags &= ~CLIENT_BLOCKED;
    c->flags = BLOCKED_NONE;
    server.bpop_blocked_clients--;

    if(!(c->flags & CLIENT_UNBLOCKED)){
        c->flags |= CLIENT_UNBLOCKED;
        listAddNodeTail(server.unblocked_clients,c);
    };
};