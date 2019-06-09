#include "server.h"

void freePubsubPattern(void *p){
    pubsubPattern *pat = p;
    decrRefCount(pat->pattern);
    zfree(pat);
};

int listMatchPubsubPattern(void *a, void *b){
    pubsubPattern *pa = a, *pb = b;

    return (pa->client == pb->client) && (equalStringObjects(pa->pattern,pb->pattern));
};

int clientSubscriptionsCount(client *c){
    return dictSize(c->pubsub_channels) + listLength(c->pubsub_patterns);
};

int pubsubSubscribeChannel(client *c, robj *channel){
    dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    if(dictAdd(c->pubsub_channels, channel, NULL) == DICT_OK){
        retval = 1;
        incrRefCount(channel);
        de = dictFind(server.pubsub_channels, channel);
        if(de == NULL){
           clients = listCreate(); 
           dictAdd(server.pubsub_channels,channel,clients);
           incrRefCount(channel);
        }else{
            clients = dictGetVal(de);
        }
        listAddNodeTail(clients,c);
    };


    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
};





