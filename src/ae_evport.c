#include <assert.h>
#include <errno.h>
#include <port.h>
#include <poll.h>


#include <sys/types.h>
#include <sys/time.h>
#include <stdio.h>


static int evport_debug = 0;


#define MAX_EVENT_BATCHSZ 512


typedef struct aeApiState{
    int portfd;
    int npending;
    int pending_fds[MAX_EVENT_BATCHSZ];
    int pending_masks[MAX_EVENT_BATCHSZ];
} aeApiState;


static int aeApiCreate(aeEventLoop *eventLoop){
    int i;
    aeApiState *state = zmalloc(sizeof(aeApiState));
    if(!state) return -1;
    
    state->portfd = port_create();
    if(state->portfd == -1){
        zfree(state);
        return -1;
    };
    
    state->npending = 0;
    
    for(i = 0; i < MAX_EVENT_BATCHSZ; i++){
        state->pending_fds[i] = -1;
        state->pending_masks[i] = AE_NONE;
    }
    
    eventLoop->apidata = state;
    return 0;
};


static int aeApiResize(aeEventLoop *eventLoop, int setsize){
    return 0;
};


static void aeApiFree(aeEventLoop *eventLoop){
    aeApiState *state = eventLoop->apidata;
    close(state->portfd);
    zfree(state);
};


static int aeApiLookupPending(aeApiState *state, int fd){
    int i;
    
    for(i = 0; i < state->npending; i++){
        if(state->pending_fds[i] == fd){
            return (i);
        };
    };  
    return (-1);
};


static int aeApiAssociate(const char *where, int portfd, int fd, int mask){
    int events = 0;
    int rv, err;
    
    if(mask & AE_READABLE){
        events |= POLLIN;
    };
    
    if(mask & AE_WRITABLE){
        events |= POLLOUT;
    };
    
    if(evport_debug){
        fprintf(stderr,"%s: port_associate(%d, 0x%x) = ", where, fd, events);
    };

    rv = port_associate(portfd, PORT_RESOURCE_FD, fd, events, (void *)(uintptr_t)mask);
    err = errno;
    
    if(evport_debug){
        fprintf(stderr, "%d (%s)\n", rv, rv == 0 ? "no error" : strerror(err));
    }; 
    
    if(rv == -1){
        fprintf(stderr, "%s: port_associate: %s\n", where, strerror(err));
        
        if(err == EAGAIN){
            fprintf(stderr, "aeApiAssociate: event port limit exceeded.");
        }

    };

};


static int aeAPiAddEvent(aeEventLoop *eventLoop, int fd, int mask){
    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;
    
    if(evport_debug){
        fprintf(stderr, "aeApiAddEvent: fd %d mask 0x%x\n", fd, mask);
    };
    
    fullmask = mask | eventLoop->events[fd].mask;
    pfd = aeApiLookupPending(state, fd);
    if(pfd == -1){
        if(evport_debug){
            fprintf(stderr, "aeApiAddEvent: adding to pending fd %d\n", fd);
        }
        state->pending_masks[pfd] |= fullmask;
        return 0;
    };
    
    return (aeApiAssociate("aeApiAddEvent", state->portfd, fd, fullmask));
};







