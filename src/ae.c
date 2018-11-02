#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "ae.h"
#include "zmalloc.h"
#include "config.h"


#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif


aeEventLoop *aeCreateEventLoop(int setsize){
    aeEventLoop *eventLoop;
    int i;
    
    if((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL){ goto err;};
    eventLoop->events = zmalloc(sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zmalloc(sizeof(aeFileEvent) * setsize);
    if(eventLoop->events == NULL || eventLoop->fired == NULL){goto err;};
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    if(aeApiCreate(eventLoop) == -1) goto err;
    for(i = 0; i < setsize; i++){
        eventLoop->events[i].mask = AE_NONE;
    };
    return eventLoop;

err:
    if(eventLoop){
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    };
    return NULL;
};


int aeGetSetSize(aeEventLoop *eventLoop){
    return eventLoop->setsize;
};


int aeResizeSetSize(aeEventLoop *eventLoop, int setsize){
    int i;
    
    if(setsize == eventLoop->setsize){return AE_OK;};
    if(eventLoop->maxfd >= setsize) return AE_ERR;
    if(aeApiResize(eventLoop, setsize) == -1) return AE_ERR;
    
    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent) *setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent) * setsize);
    eventLoop->setsize = setsize;
    
    for(i = eventLoop->maxfd + 1; i < setsize; i++){
        eventLoop->events[i].mask = AE_NONE;
    }
    return AE_OK;
};


void aeDeleteEventLoop(aeEventLoop *eventLoop){
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
};

void aeStop(aeEventLoop *eventLoop){
    eventLoop->stop = 1;
};


int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, aeFileProc *proc, void *clientData){
    if(fd >= eventLoop->setsize){
        errno = ERANGE;
        return AE_ERR;
    };
    aeFileEvent *fe = &eventLoop->events[fd];
    if(aeApiAddEvent(eventLoop,fd,mask) == -1){
        return AE_ERR;
    }; 
    
    fe->mask |= mask;
    if(mask & AE_READABLE){fe->rfileProc = proc;};
    if(mask & AE_WRITABLE){fe->wfileProc = proc;};
    
    fe->clientData = clientData;
    if(fd > eventLoop->maxfd){
        eventLoop->maxfd = fd;
    }; 
    return AE_OK;
};

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask){
    if(fd >= eventLoop->setsize) return;
    aeFileEvent *fe = &eventLoop->events[fd];
    if(fe->mask == AE_NONE) return;
    
    aeApiDelEvent(eventLoop,fd,mask);
    fe->mask = fe->mask & (~mask);
    if(fd == eventLoop->maxfd && fe->mask == AE_NONE){
        int j;
        
        for(j = eventLoop->maxfd -1; j >= 0; j--){
            if(eventLoop->events[j].mask != AE_NONE) break;
        };
        eventLoop->maxfd = j;
    };
};

int aeGetFileEvents(aeEventLoop *eventLoop, int fd){
    if(fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];
    return fe->mask;
};


static void aeGetTime(long *seconds, long *milliseconds){
    struct timeval tv;
    
    gettimeofday(&tv,NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;    
};

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms){
    long cur_sec, cur_ms, when_sec, when_ms;
    
    aeGetTime(&cur_sec,&cur_ms);    
    when_sec = cur_sec + milliseconds/1000;
    when_ms = cur_ms + milliseconds %1000;
    if(when_ms >= 1000){
        when_sec++;
        when_ms -= 1000;
    } 
    *sec = when_sec;
    *ms = when_ms;
};



long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData, aeEventFinalizeProc *finalizerProc){
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;
    
    te = zmalloc(sizeof(*te));
    if(te == NULL) return AE_ERR;
   
    te->id = id;
    aeAddMillisecondsToNow(milliseconds, &te->when_sec, &te->when_ms);  
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
};


int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id){
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te){
        if(te->id == id){
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    };
    return AE_ERR;
};


static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop){
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest  = NULL;
    
    while(te){
        if(!nearest || te->when_sec < nearest->when_sec || (te->when_sec == nearest->when_sec && te->when_ms < nearest->when_ms)){
            nearest = te;
        }
    };
    return nearest;
};

static int processTimeEvents(aeEventLoop *eventLoop){
    int processed = 0;
    aeTimeEvent *te, *prev;
    long long maxId;
    time_t now = time(NULL);
    
    if(now < eventLoop->lastTime){
        te = eventLoop->timeEventHead;
        while(te){
            te->when_sec = 0;
            te = te->next;
        };
    };    
    eventLoop->lastTime = now;
    
    prev = NULL;
    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId - 1;
    while(te){
        long now_sec, now_ms;
        long long id;
        
        if(te->id == AE_DELETED_EVENT_ID){
            aeTimeEvent *next = te->next;
            if(prev == NULL){
                eventLoop->timeEventHead = te->next;
            }else{
                prev->next = te->next;
            }
    
            if(te->finalizerProc){
                te->finalizerProc(eventLoop, te->clientData);
            };
            zfree(te);
            te = next;
            continue;
        };
        
        if(te->id > maxId){
            te = te->next;
            continue;
        };
        
        aeGetTime(&now_sec, &now_ms);
        if(now_sec > te->when_sec || (now_sec == te->when_sec && now_ms >= te->when_ms)){
            int retval;
            
            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            if(retval != AE_NOMORE){
                aeAddMillisecondsToNow(retval, &te->when_sec, &te->when_ms);
            }else{
                te->id = AE_DELETED_EVENT_ID;
            }
            
        };
    
        prev = te;
        te = te->next;
    };
    return processed; 
    
};

int aeProcessEvents(aeEventLoop *eventLoop, int flags){
    int processed = 0, numevents;
    if(!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)){return 0;};
    if(eventLoop->maxfd != -1 || ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))){
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;
        if(flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT)){
            shortest = aeSearchNearestTimer(eventLoop);
        };
        if(shortest){
            long now_sec, now_ms;
            
            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;
            
            long long ms = (shortest->when_sec - now_sec) * 1000 + shortest->when_ms - now_ms;
            
            if(ms > 0){
                tvp->tv_sec = ms/1000;
                tvp->tv_usec = (ms%1000) * 1000;
            }else{
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        }else{
            if(flags & AE_DONT_WAIT){
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            }else{
                tvp = NULL;
            }
        };
        
        numevents = aeApiPoll(eventLoop,tvp);
        for(j = 0; j < numevents; j++){
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int rfired = 0;
            
            if(fe->mask & mask & AE_READABLE){
                rfired = 1;
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
            };
            
            if(fe->mask & mask & AE_WRITABLE){
                if(!rfired || fe->wfileProc != fe->rfileProc){
                    fe->wfileProc(eventLoop, fd, fe->clientData,mask);
                };
            };
            processed++;
        };
    };
    
    if(flags & AE_TIME_EVENTS){
        processed += processTimeEvents(eventLoop);
    };
    return processed;

};



int aeWait(int fd, int mask, long long milliseconds){
    struct pollfd pfd;
    int retmask = 0, retval;
    memset(&pfd, 0, sizeof(pfd));

    pfd.fd = fd;
    
    if(mask & AE_READABLE) pfd.events |= POLLIN;
    if(mask & AE_WRITABLE) pfd.events |= POLLOUT;
    
    if((retval = poll(&pfd, 1, milliseconds)) == 1){
        if(pfd.revents & POLLIN) retmask |= AE_READABLE;
        if(pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
    
        if(pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if(pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
    
        return retmask;
    }else{
        return retval; 
    };
};


void aeMain(aeEventLoop *eventLoop){
    eventLoop->stop = 0;
    while(!eventLoop->stop){
       if(eventLoop->beforesleep != NULL){
            eventLoop->beforesleep(eventLoop);
        } 
       aeProcessEvents(eventLoop, AE_ALL_EVENTS);
    };
};

char *aeGetApiName(void){
    return aeApiName();
};

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep){
    eventLoop->beforesleep = beforesleep;
};
