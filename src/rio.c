#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h" 
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"


static size_t rioBufferWrite(rio *r, const void *buf, size_t len){
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr, (char *)buf, len);
    r->io.buffer.pos += len;
    return 1;
};


static size_t rioBufferRead(rio *r, void *buf, size_t len){
    if(sdslen(r->io.buffer.ptr) - r->io.buffer.pos < len){
        return 0;
    }
    memcpy(buf,r->io.buffer.ptr + r->io.buffer.pos, len);
    r->io.buffer.pos += len;
    return 1;
};


static off_t rioBufferTell(rio *r){
    return r->io.buffer.pos;
};

static int rioBufferFlush(rio *r){
    UNUSED(r);
    return 1;
};


static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,
    0,
    0,0,
    {
        {
            NULL,0
        }
    }
};

void rioInitWithBuffer(rio *r, sds s){
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
};

static size_t rioFileWrite(rio *r, const void *buf, size_t len){
    size_t retval;
    
    retval = fwrite(buf,len,1,r->io.file.fp);
    r->io.file.buffered += len;
    
    if(r->io.file.autosync &&
       r->io.file.buffered >= r->io.file.autosync){
            fflush(r->io.file.fp);
            // fsync under linux and fsync in other systes
            aof_fsync(fileno(r->io.file.fp));
            r->io.file.buffered = 0;
       };
    return retval;
};


static size_t rioFileRead(rio *r, void *buf, size_t len){
    return fread(buf,len,1,r->io.file.fp);
};


static off_t rioFileTell(rio *r){
    return ftello(r->io.file.fp);
};


static int rioFileFlush(rio *r){
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
};

static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,
    0,
    0,
    0,
    {{NULL, 0}}
}


void rioInitWithFile(rio *r, FILE *fp){
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
};

static size_t rioFdsetWrite(rio *r, const void *buf, size_t len){
    ssize_t retval;
    int j;
    unsigned char *p = (unsigned char*) buf;
    int doflush = (buf == NULL && len == 0);
    
    if(len){
        r->io.fdset.buf = sdscatlen(r->io.fdset.buf,buf,len);
        len = 0;
        if(sdslen(r->io.fdset.buf) > PROTO_IOBUF_LEN) doflush = 1;
    };
    
    if(doflush){
        p = (unsigned char *) r->io.fdset.buf;
        len = sdslen(r->io.fdset.buf);
    };
    
    while(len){
        size_t count = len < 1024 ? len : 1024;
        int broken = 0;
        for(j = 0; j < r->io.fdset.numfds;j++){
            if(r->io.fdset.state[j] != 0){
                broken++;
                continue;
            };
            
            size_t nwritten = 0;
            while(nwritten != count){
                retval = write(r->io.fdset.fds[j], p + nwritten, count - nwritten);
                if(retval <= 0){
                    if(retval == -1 && errno == EWOULDBLOCK){errno = ETIMEDOUT;};
                    break;
                };
                nwritten += retval;
            };
            
            if(nwritten != count){
                r->io.fdset.state[j] = errno;
                if(r->io.fdset.state[j] == 0) r->io.fdset.state[j] = EIO;
            }
        };
        if(broken == r->io.fdset.numfds) return 0;
        p+= count;
        len -= count;
        r->io.fdset.pos += count;
    };
    if(doflush){
        sdsclear(r->io.fdset.buf);
    };
    return 1;
};

static size_t rioFdsetRead(rio *r, void *buf, size_t len){
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0;
};

static off_t rioFdsetTell(rio *r){
    return r->io.fdset.pos;
};


static int rioFdsetFlush(rio *r){
    return rioFdsetWrite(r,NULL,0);
};

static const rio rioFdsetIO = {
    rioFdsetRead,
    rioFdsetWrite,
    rioFdsetTell,
    rioFdsetFlush,
    NULL,
    0,
    0,
    0,
    { { NULL, 0} } 
};

void rioInitWithFdset(rio *r, int *fds, int numfds){
    int j;
    
    *r  = rioFdsetIO;
    r->io.fdset.fds = zmalloc(sizeof(int) * numfds);
    r->io.fdset.state = zmalloc(sizeof(int) * numfds);
    memcpy(r->io.fdset.fds,fds,sizeof(int)*numfds);
    for(j = 0; j < numfds;j++){r->io.fdset.state[j] = 0;};
    r->io.fdset.numfds = numfds;
    r->io.fdset.pos = 0;
    r->io.fdset.buf = sdsempty();
};

void rioFreeFdset(rio *r){
    zfree(r->io.fdset.fds);
    zfree(r->io.fdset.state);
    zfree(r->io.fdset.buf);
};


void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len){
    r->cksum = crc64(r->cksum,buf,len);
};


void rioSetAutoSync(rio *r, off_t bytes){
    serverAssert(r->read == rioFileIO.read);
    r->io.file.autosync = bytes;
};

size_t rioWriteBulkCount(rio *r, char prefix, int count){
    char cbuf[128];
    int clen;
    
    cbuf[0] = prefix;
    clen = 1 + ll2string(cbuf + 1, sizeof(cbuf) - 1, count); 
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    if(rioWrite(r,cbuf,clen) == 0) {return 0;};
    return clen;
};

size_t rioWriteBulkString(rio *r, const char *buf, size_t len){
    size_t nwritten;
    if((nwritten = rioWriteBulkCount(r,'$',len)) == 0){return 0;};
    if(len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if(rioWrite(r,"\r\n",2)==0) return 0;
    return nwritten + len + 2;
}

size_t rioWriteBulkLongLong(rio *r, long long l){
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l); 
    return rioWriteBulkString(r,lbuf,llen);
};


size_t rioWriteBulkDouble(rio *r, double d){
    char dbuf[128];
    unsigned int dlen;
    
    dlen = snprintf(dbuf, sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
};









