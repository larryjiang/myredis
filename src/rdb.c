#include "server.h"
#include "lzf.h"
#include "zipmap.h"
#include "endianconv.h"
#include <math.h>
#include <sys/types.h> 
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h> 
#define rdbExitReportCorruptRDB(...) rdbCheckThenExit(__LINE__,__VA_ARGS__)


extern rdbCheckMode;
void rdbCheckError(const char *fmt, ...);
void rdbCheckSetError(const char *fmt, ...);


void rdbCheckThenExit(int linenum, char *reason,...){
    va_list ap;
    char msg[1024];
    int len;


    len = snprintf(msg, sizeof(msg),"Internal error in RDB reading function at rdb.c:%d ->",linenum);
    va_start(ap,reason);
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
    va_end(ap);

    if(!rdbCheckMode){
        serverLog(LL_WARNING,"%s",msg); 
        char *argv[2] = {"",server.rdb_filename};
        redis_check_rdb_main(2,argv);
    }else{
        rdbCheckError("%s",msg); 
    }
    exit(1);
};


static int rdbWriteRaw(rio *rdb, void *p, size_t len){
    if(rdb && rioWrite(rdb, p, len) == 0){
        return -1; 
    };
    return len;
};


int rdbSaveType(rio *rdb, unsigned char type){
    return rdbWriteRaw(rdb,&type,1);
}

int rdbLoadType(rio *rdb){
    unsigned char type;
    if(rioRead(rdb,&type,1) == 0){
        return -1; 
    }
    return type;
}

time_t rdbLoadTime(rio *rdb){
    int32_t t32;
    if(rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
};

int rdbSaveMillisecondTime(rio *rdb, long long t){
    int64_t t64 = (int64_t) t;
    return rdbWriteRaw(rdb,&t64,8);
}

long long rdbLoadMillisecondTime(rio *rdb){
    int64_t t64;
    if(rioRead(rdb,&t64,8) == 0) return -1;
    return (long long)t64;
}

int rdbSaveLen(rio *rdb, uint64_t len){
    unsigned char buf[2];
    size_t nwritten;

    if(len < (1<<6)){
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6) 
        if(rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    }else if(len < (1<<14)){
        buf[0] = ((len>>8)&0xFF) | (RDB_14BITLEN<<6) 
        buf[1] = len & 0xFF;
        if(rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    }else if(len <= UINT32_MAX){
        buf[0] = RDB_32BITLEN;
        if(rdbWriteRaw(rdb,buf,1) == -1) return -1; 
        uint32_t len32 = htonl(len);
        if(rdbWriteRaw(rdb,&len32,4) == -1) return -1;
        nwritten = 1 + 4;
    }else{
        buf[0] = RDB_64BITLEN;
        if(rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonu64(len); 
        if(rdbWriteRaw(rdb,&len,8) == -1) return -1;
        nwritten = 1 + 8;
    }
    return nwritten;
};

int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr){
    unsigned char buf[2];
    int type;

    if(isencoded) *isencoded = 0;
    if(rioRead(rdb,buf,1) == 0) return -1;
    type = (buf[0] & 0xC0)>>6;
    if(type == RDB_ENCVAL){
        if(isencoded) *isencoded = 1;  
        *lenptr = buf[0] &0x3F;
    }else if(type == RDB_6BITLEN){
        *lenptr = buf[0]&0x3F; 
    }else if(type == RDB_14BITLEN){
        if(rioRead(rdb,buf+1,1) == 0){
            return -1; 
        } 
        *lenptr = ((buf[0]&0x3F) << 8) | buf[1];
    }else if(buf[0] == RDB_32BITLEN){
        uint32_t len;
        if(rioRead(rdb,&len,4) == 0){
            return -1; 
        } 
        *lenptr = ntohl(len);
    }else if(buf[0] == RDB_64BITLEN){
        uint64_t len;
        if(rioRead(rdb,&len,8) == 0) return -1;
        *lenptr = ntohu64(len);
   }else{
        rdbExitReportCorruptRDB("Unknown length encoding %d in rdbLoadLen()",type); 
        return -1;
   } 
   return 0;
};


uint64_t rdbLoadLen(rio *rdb, int *isencoded){
    uint64_t len;
    if(rdbLoadLenByRef(rdb,isencoded,&len) == -1) return RDB_LENERR;
    return len;
}

int rdbEncodeInteger(long long value, unsigned char *enc){
    if(value >= -(1<<7) && value <= (1<<7)-1){
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
        enc[1] = value & 0xFF; 
        return 2;
    }else if(value >= -(1<<15) && value <= (1<<15)-1){
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16; 
        enc[1] = value & 0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    }else if(value >= -((long long)1<<31) && value <= ((long long)1<<31)-1){
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32; 
        enc[1] = value & 0xFF;
        enc[2] = (value>>8) & 0xFF;
        enc[3] = (value>>16) & 0xFF;
        enc[4] = (value>>24) & 0xFF;
        return 5;
    }else{
        return 0; 
    }
};

void *rdbLoadIntegerObject(rio *rdb, int enctype, int flags, size_t *lenptr){
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;


    if(enctype == RDB_ENC_INT8){
        if(rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0]; 
    }else if(enctype == RDB_ENC_INT16){
        uint16_t v;
        if(rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0] | (enc[1]<<8) 
        val = (int16_t)v;
    }else if(enctype == RDB_ENC_INT32){
        uint32_t v;
        if(rioRead(rdb,enc,4) ==0 ) return NULL; 
        v = enc[0] | (enc[1]<<8) | (enc[2]<<16) | (enc[3]<<24);
        val = (int32_t) v;
    }else{
        val = 0;
        rdbExitReportCorruptRDB("Unknown RDB integer encoding type %d",enctype); 
    }
   
    if(plain || sds){
        char buf[LONG_STR_SIZE], *p; 
        int len = ll2string(buf,sizeof(buf),val);
        if(lenptr) *lenptr = len;
        p = plain ? zmalloc(len) : sdsnewlen(NULL,len);
        memcpy(p,buf,len)
        return p;
    }else if(encode){
        return createStringObjectFromLongLong(val); 
    }else{
        return createObject(OBJ_STRING,sdsfromlonglong(val)); 
    } 
};

int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc){
    long long value;
    char *endptr, buf[32];

    value = strtoll(s,&endptr,10);
    if(endptr[0] != '\0') return 0;
    ll2string(buf,32,value);
    if(strlen(buf)!= len || memcmp(buf,s,len)) return 0;
    return rdbEncodeInteger(value,enc);
}

ssize_t rdbSaveLzfBlob(rio *rdb, void *data, size_t compress_len, size_t original_len){
    unsigned char byte;
    ssize_t n, nwritten = 0;

    byte = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
    if((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    if((n = rdbSaveLen(rdb, compress_len)) == -1) goto writeerr;
    nwritten += n;

    if((n = rdbSaveLen(rdb, original_len)) == -1) goto writeerr;
    nwritten += n;

    if((n = rdbWriteRaw(rdb,data,compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;
    
writeerr:
    return -1;

}

ssize_t rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len){
    size_t comprlen, outlen;
    void *out;

    if(len <= 4) return 0;
    outlen = len - 4; 
    if((out = zmalloc(outlen + 1)) == NULL) return 0;
    comprlen = lzf_compress(s,len,out,outlen);
    if(comprlen == 0){
        zfree(out); 
        return 0;
    }
    ssize_t nwritten = rdbSaveLzfBlob(rdb,out,comprlen,len);
    zfree(out);
    return nwritten;
};

void *rdbLoadLzfStringObject(rio *rdb, int flags, size_t *lenptr){
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    uint64_t len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if((clen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if((c = zmalloc(clen)) == NULL) goto err;
    if(plain){
        val = zmalloc(len); 
        if(lenptr) *lenptr = len;
    }else{
        val = sdsnewlen(NULL,len); 
    };
    
    if(rioRead(rdb,c,clen) == 0) goto err;
    if(lzf_decompress(c,clen,val,len) == 0){
        if(rdbCheckMode) rdbCheckSetError("Invalid LZF compressed string"); 
        goto err;
    }
    zfree(c);
    if(plain || sds){
        return val; 
    }else{
        return createObject(OBJ_STRING,val); 
    }

err:
    zfree(c);
    if(plain){
        zfree(val); 
    }else{
        sdsfree(val); 
    }
    return NULL;
};

ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len){
    int enclen;
    ssize_t n, nwritten = 0;

    if(len <= 11){
        unsigned char buf[5]; 
        if((enclen = rdbTryIntegerEncoding((char *)s,len,buf)) > 0){
            if(rdbWriteRaw(rdb,buf,enclen) == -1) return -1; 
            return enclen;
        }
    }


    if(server.rdb_compression && len > 20){
        n = rdbSaveLzfStringObject(rdb,s,len); 
        if(n == -1) return -1;
        if(n > 0) return n;
    }
    
    if((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;
    if(len > 0){
        if(rdbWriteRaw(rdb,s,len) == -1) return -1; 
        nwritten += len;
    }
    return nwritten;
};

ssize_t rdbSaveLongLongAsStringObject(rio *rdb, long long value){
    unsigned char buf[32];
    ssize_t n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if(enclen > 0){
        return rdbWriteRaw(rdb,buf,enclen); 
    }else{
        enclen = ll2string((char*)buf, 32, value); 
        serverAssert(enclen < 32);
        if((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n; 
        if((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
};

int rdbSaveStringObject(rio *rdb, robj *obj){

    if(obj->encoding == OBJ_ENCODING_INT){ 
       return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr); 
    }else{
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj)); 
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
};

void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr){
    int encode = flags & RDB_LOAD_ENC;
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int isencoded;
    uint64_t len;

    len = rdbLoadLen(rdb,&isencoded);
    if(isencoded){
        switch(len){
            case RDB_ENC_INT8:
            case RDB_ENC_INT16:
            case RDB_ENC_INT32:
                return rdbLoadIntegerObject(rdb,len,flags,lenptr);    
            case RDB_ENC_LZF:
                return rdbLoadLzfStringObject(rdb,flags,lenptr);
            default:
                rdbExitReportCorruptRDB("Unknow RDB string encoding type %d",len);
        }; 
    };

    if(len == RDB_LENERR) return NULL;
    if(plain || sds){
        void *buf = plain ? zmalloc(len) : sdsnewlen(NULL,len); 
        if(lenptr) *lenptr = len;
        if(len && rioRead(rdb,buf,len) == 0){
            if(plain){
                zfree(buf); 
            }else{
                sdsfree(buf); 
            } 
            return NULL;
        };
        return buf; 
    }else{
        robj *o = encode ? createRawStringObject(NULL, len) : createRawStringObject(NULL,len); 
        if(len && rioRead(rdb,o->ptr,len) == 0){
            decrRefCount(o); 
            return NULL;
        }
        return o; 
    };
};


robj *rdbLoadStringObject(rio *rdb){
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
};

robj *rdbLoadEncodedStringObject(rio *rdb){
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_ENC,NULL);
}; 

int rdbSaveDoubleValue(rio *rdb, double val){
    unsigned char buf[128];
    int len;

    if(isnan(val)){
        buf[0] = 253;  
        len = 1;
    }else if(!isfinite(val)){
        len = 1; 
        buf[0] = (val < 0) 255 : 254;
    }else{
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0X7fffffffffffffffLL) 
    double min = -4503599627370495;
    double max = 4503599627370495; 
    if(val > min && val < max && val == ((double)((long long)val))){
        ll2string((char*)buf+1, sizeof(buf) - 1, (long long)val); 
    }else
#endif
    snprintf((char*)buf + 1, sizeof(buf) - 1, "%.17g",val);
    buf[0] = strlen((char*)buf + 1);
    len = buf[0] + 1;
    }
    return rdbWriteRaw(rdb,buf,len);
};


int rdbLoadDoubleValue(rio *rdb, double *val){
    char buf[256];
    unsigned char len;

    if(rioRead(rdb,&len,1) == 0) return -1;
    switch(len){
        case 255: *val = R_NegInf; return 0;
        case 254: *val = R_PosInf; return 0; 
        case 253: *val = R_Nan; return 0;
    default:
        if(rioRead(rdb,buf,len) == 0) return -1; 
        buf[len] = '\0';
        sscanf(buf,"%lg",val);
        return 0;
    };
};

int rdbSaveBinaryDoubleValue(rio *rdb, double val){
    memrev64ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
};

int rdbLoadBinaryDoubleValue(rio *rdb, double *val){
    if(rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
};

int rdbSaveBinaryFloatValue(rio *rdb, float val){
    memrev32ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
};


int rdbLoadBinaryFloatValue(rio *rdb, float *val){
    if(rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev32ifbe(val);
    return 0;
};

int rdbSaveObjectType(rio *rdb, robj *o){
    switch(o->type){
        case OBJ_STRING:
           return rdbSaveType(rdb,RDB_TYPE_STRING); 
        case OBJ_LIST:
           if(o->encoding == OBJ_ENCODING_QUICKLIST){
                return rdbSaveType(rdb,RDB_TYPE_LIST_QUICKLIST);
           }else{
                serverPanic("Unknown list encoding");        
           }
        case OBJ_SET:
           if(o->encoding == OBJ_ENCODING_INTSET){
                return rdbSaveType(rdb,RDB_TYPE_SET_INTSET); 
           }else if(o->encoding == OBJ_ENCODING_HT){
                return rdbSaveType(rdb,RDB_TYPE_SET); 
           }else{
                serverPanic("Unknown set encoding"); 
           }
        case OBJ_ZSET:
           if(o->encoding == OBJ_ENCODING_ZIPLIST){
                return rdbSaveType(rdb,RDB_TYPE_ZSET_ZIPLIST); 
           }else if(o->encoding == OBJ_ENCODING_SKIPLIST){
                return rdbSaveType(rdb,RDB_TYPE_ZSET_2); 
           }else{
                serverPanic("Unknown sorted set encoding"); 
           } 
        case OBJ_HASH: 
           if(o->encoding == OBJ_ENCODING_ZIPLIST){
                return rdbSaveType(rdb,RDB_TYPE_HASH_ZIPLIST); 
           }else if(o->encoding == OBJ_ENCODING_HT){
                return rdbSaveType(rdb,RDB_TYPE_HASH);  
           }else{
                serverPanic("Unknown object type"); 
           }
       case OBJ_MODULE:
           return rdbSaveType(rdb,RDB_TYPE_MODULE);
       default:
           serverPanic("Unknown object type");
    };
    return -1;
};

int rdbLoadObjectType(rio *rdb){
    int type;
    if((type = rdbLoadType(rdb)) == -1) return -1;
    if(!rdbIsObjectType(type)) return -1;
    return type;
};

ssize_t rdbSaveObject(rio *rdb, robj *o){
    ssize_t n = 0, nwritten = 0; 
    if(o->type == OBJ_STRING){
        if((n = rdbSaveStringObject(rdb,o)) == -1) return -1; 
        nwritten += n;
    }else if(o->type == OBJ_LIST){
        if(o->encoding == OBJ_ENCODING_QUICKLIST){
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head; 
            
            if((n = rdbSaveLen(rdb,ql->len)) == -1) return -1;
            nwritten += n;
            do{
                if(quicklistNodeIsCompressed(node)){
                    void *data;
                    size_t compress_len = quicklistGetLzf(node,&data); 
                    if((n = rdbSaveLzfBlob(rdb,data,compress_len,node->sz)) == -1) return -1;
                    nwritten += n;
                }else{
                    if((n = rdbSaveRawString(rdb,node->zl,node->sz)) == -1) return -1; 
                    nwritten += n;
                }   
            }while(); 
        }else{
            serverPanic("Unknown list encoding"); 
        } 
    }else if(o->type == OBJ_SET){
        if(o->encoding == OBJ_ENCODING_HT){
            dict *set = o->ptr; 
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if((n = rdbSaveLen(rdb,dictSize(set))) == -1) return -1;
            nwritten += n;
            while((de = dictNext(di)) != NULL){
                sds ele = dictGetKey(de); 
                if((n = rdbSaveRawString(rdb,(unsigned char *)ele, sdslen(ele))) == -1){
                    return -1; 
                };
                nwritten += n;
            }; 
            dictReleaseIterator(di);
        }else if(o->type == OBJ_ENCODING_INTSET){
            size_t l  = intsetBlobLen((intset*)o->ptr); 
            if((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        }else{
            serverPanic("Unkown set encoding"); 
        } 
    }else if(o->type == OBJ_ZSET){
        if(o->encoding == OBJ_ENCODING_ZIPLIST){
            size_t l = ziplistBlobLen((unsigned char*)o->ptr); 
            if((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        }else if(o->encoding == OBJ_ENCODING_SKIPLIST){
            zset *zs = o->ptr; 
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            if((n = rdbSaveLen(rdb,dictSize(zs->dict))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL){
                sds ele = dictGetKey(de); 
                double *score = dictGetVal(de);
                if((n = rdbSaveRawString(rdb,(unsigned char *)ele, sdslen(ele))) == -1) return -1;
                nwritten += n;
                if((n = rdbSaveBinaryDoubleValue(rdb,*score)) == -1) return -1; 
                nwritten += n;
            };
            dictReleaseIterator(di);
        }else{
            serverPanic("Unknown sorted set encoding"); 
        } 
    }else if(o->type == OBJ_HASH){
       if(o->encoding == OBJ_ENCODING_ZIPLIST){
            size_t l = ziplistBlobLen((unsigned char*)o->ptr); 
            if((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
       }else if(o->encoding == OBJ_ENCODING_HT){
        dictIterator *di = dictGetIterator(o->ptr); 
        dictEntry *de;
        if((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) return -1;
        nwritten += n;
        while((de = dictNext(di)) != NULL){
            sds field = dictGetKey(de); 
            sds value = dictGetVal(de);
            if((n = rdbSaveRawString(rdb,(unsigned char *)field, sdslen(field))) == -1) return -1;
            nwritten += n;
            if((n = rdbSaveRawString(rdb,(unsigned char *)value, sdslen(value))) == -1) return -1;
            nwritten += n;
        };
        dictReleaseIterator(di);
       }else{
            serverPanic("Unknown hash encoding"); 
       } 
    }else if(o->type == OBJ_MODULE){
        RedisModuleIO io;
        moduleValue *mv = o->ptr; 
        moduleType *mt = mv->type;
        moduleInitIOContext(io,mt,rdb);

        int retval = rdbSaveLen(rdb,mt->id);
        if(retval == -1) return -1;
        io.bytes += retval;
    
        mt->rdb_save(&io,mv->value); 
        if(io.ctx){
            moduleFreeContext(io.ctx); 
            zfree(io.ctx);
        }
        return io.error ? -1 : (ssize_t) io.bytes; 
    }else{
        serverPanic("Unknown object type"); 
    };
};

size_t rdbSavedObjectLen(robj *o){
    ssize_t len = rdbSaveObject(NULL,o);
    serverAssertWithInfo(NULL,o,len != -1);
    return len;
}

int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, long long expiretime, long long now){
    if(expiretime != -1){
        if(expiretime < now) return 0; 
        if(rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if(rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }; 
    
    if(rdbSaveObjectType(rdb,val) == -1) return -1;
    if(rdbSaveStringObject(rdb,key) == -1) return -1;
    if(rdbSaveObject(rdb,val) == -1) return -1;

    return 1;
};


int rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen){
    if(rdbSaveType(rdb,RDB_OPCODE_AUX) == -1) return -1;
    if(rdbSaveRawString(rdb,key,keylen) == -1) return -1;
    if(rdbSaveRawString(rdb,val,vallen) == -1) return -1;
    return 1;
};

int rdbSaveAuxFieldStrInt(rio *rdb, char *key, long long val){
    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
};

int rdbSaveInfoAuxFields(rio *rdb, int flags, rdbSaveInfo *rsi){
    int redis_bits = (sizeof(void *) == 8) ? 64 : 32; 
    int aof_preamble = (flags & RDB_SAVE_AOF_PREAMBLE) != 0;
    if(rdbSaveAuxFieldStrStr(rdb,"redis-ver",REDIS_VERSION) == -1) return -1;
    if(rdbSaveAuxFieldStrInt(rdb,"redis-bits",redis_bits) == -1) return -1;
    if(rdbSaveAuxFieldStrInt(rdb,"ctime",time(NULL)) == -1) return -1;
    if(rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) return -1;

    
    if(rsi){
        if(rsi->repl_stream_db && rdbSaveAuxFieldStrInt(rdb,"repl-stream-db",rsi->repl_stream_db) == -1){
            return -1; 
        }; 
    }

    if(rdbSaveAuxFieldStrInt(rdb,"aof-preamble",aof_preamble) == -1) return -1;
    if(rdbSaveAuxFieldStrStr(rdb,"repl-id",server.replid) == -1) return -1;
    if(rdbSaveAuxFieldStrInt(rdb,"repl-offset",server.master_repl_offset) == -1) return -1;
    return 1;
};


int rdbSaveRio(rio *rdb, int *error, int flags, rdbSaveInfo *rsi){
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];


    int j;

    long long now = mstime();
    uint64_t cksum;
    size_t processed = 0;

    if(server.rdb_checksum){
        rdb->update_cksum = rioGenericUpdateChecksum; 
    }
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
    if(rdbWriteRaw(rdb,magic,9) == -1) goto werr;
    if(rdbSaveInfoAuxFields(rdb,flags,rsi) == -1) goto werr;

    for(j = 0; j < server.dbnum; j++){
        redisDb *db = server.db + j; 
        dict *d = db->dict;
        if(dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if(!di) return C_ERR;
        
        if(rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) goto werr;
        if(rdbSaveLen(rdb,j) == -1) goto werr;
        uint32_t db_size, expires_size;
        db_size = (dictSize(db->dict) <= UINT32_MAX) ? dictSize(db->dict) : UINT32_MAX;
        expires_size = (dictSize(db->expires) <= UINT32_MAX) ? dictSize(db->expires) : UINT32_MAX; 
        if(rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) goto werr;
        if(rdbSaveLen(rdb,db_size) == -1) goto werr;
        if(rdbSaveLen(rdb,expires_size) == -1) goto werr;


        while((de = dictNext(di)) != NULL){
            sds keystr = dictGetKey(de); 
            robj key, *o = dictGetVal(de);
            long long expire;


            initStaticStringObject(key,keystr);
            expire = getExpire(db,&key);
            if(rdbSaveKeyValuePair(rdb,&key,o,expire,now) == -1) goto werr;
            if(flags & RDB_SAVE_AOF_PREAMBLE && 
                    rdb->processed_bytes > processed + AOF_READ_DIFF_INTERVAL_BYTES){
                processed = rdb->processed_bytes;
                aofReadDiffFromParent(); 
            }
        };
        dictReleaseIterator(di);
    };
    di = NULL;
    if(rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;

    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    if(rioWrite(rdb,&cksum,8) == 0) goto werr;
    return C_OK;


werr:
    if(error) *error = errno;
    if(di) dictReleaseIterator(di);
    return C_ERR;
};


int rdbSaveRioWithEOFMark(rio *rdb, int *error, rdbSaveInfo *rsi){
    char eofmark[RDB_EOF_MARK_SIZE];
    getRandomHexChars(eofmark,RDB_EOF_MARK_SIZE);
    if(error) *error = 0;
    if(rioWrite(rdb,"$EOF:",5) == 0) goto werr;
    if(rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    if(rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if(rdbSaveRio(rdb,error,RDB_SAVE_NONE,rsi) == C_ERR) goto werr;
    if(rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    return C_OK;
werr:
    if(error && *error == 0) *error = errno;
    return C_ERR;
};


int rdbSave(char *filename, rdbSaveInfo *rsi){
    char tmpfile[256];
    char cwd[MAXPATHLEN];
    FILE *p;
    rio rdb;
    int error = 0;
    
    snprintf(tmpfile,256,"tmp-%d.rdb",(int)getpid());
    fp = fopen(tmpfile,"w");
    if(!fp){
       char *cwdp = getcwd(cwd,MAXPATHLEN);      
       serverLog(LL_WARNING,"Error moving temp DB file %s on the final destination %s (in server root dir %s): %s", tmpfile,filename,cwdp ? cwdp : "unknown", strerror(errno));
       unlink(tmpfile);
       return C_ERR; 
    }

    rioInitWithFile(&rdb,fp);
    if(rdbSaveRio(&rdb,&error,RDB_SAVE_NONE,rsi) == C_ERR){
        errno = error;
        goto werr;
    };
    
    if(fflush(fp) == EOF) goto werr;
    if(fsync(fileno(fp)) == -1) goto werr;
    if(fclose(fp) == EOF) goto werr;

    if(rename(tmpfile,filename) == -1){
        char *cwdp = getcwd(cwd,MAXPATHLEN); 
        serverLog(LL_WARNING, "Error moving temp DB file %s on the final destination %s (in server root dir %s): %s",tmpfile,filename,cwdp ? cwdp : "unknown",strerror(errno));
        unlink(tmpfile);
        return C_ERR;
    };

    serverLog(LL_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = C_OK;
    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s",strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
};


int rdbSaveBackground(char *filename, rdbSaveInfo *rsi){
    pid_t childpid;
    long long start;
    if(server.aof_child_pid != -1 || server.rdb_child_pid != -1) return C_ERR;
    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = time(NULL);
    openChildInfoPipe();
    start = ustime(); 
    if((childpid = fork()) == 0){
        int retval; 
        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-bgsave");
        retval = rdbSave(filename,rsi);
        if(retval == C_OK){
            size_t private_dirty = zmalloc_get_private_dirty(-1); 
            if(private_dirty){
                serverLog(LL_NOTICE, "RDB: %zu MB of memory used by copy-on-write", private_dirty/(1024*1024)); 
            }
            server.child_info_data.cow_size = private_dirty;
            sendChildInfo(CHILD_INFO_TYPE_RDB);
        }; 
        exitFromChild((retval == C_OK) ? 0 : 1);
    }else{
        server.stat_fork_time = ustime() - start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024 * 1024 * 1024);
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if(childpid == -1){
            closeChildInfoPipe(); 
            server.lastbgsave_status = C_ERR;
            serverLog(LL_WARNING,"Can't save in background: fork:%s",strerror(errno));
            return C_ERR;
        };
        serverLog(LL_NOTICE,"Background saving started by pid %d",childpid);
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_pid = childpid;
        server.rdb_child_type = RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    };
    return C_OK;
};

void rdbRemoveTempFile(pid_t childpid){
    char tmpfile[256];
    snprintf(tmpfile,sizeof(tmpfile),"tmp-%d.rdb",(int)childpid);
    unlink(tmpfile);
};


robj *rdbLoadObject(int rdbtype, rio *rdb){
    robj *o = Null, *ele, *dec; 
    uint64_t len;
    unsigned int i;

    if(rdbtype == RDB_TYPE_STRING){
        if((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    }else if(rdbtype == RDB_TYPE_LIST){
        if((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL; 
        o = createQuicklistObject();
        quicklistSetOptions(o->ptr,server.list_max_ziplist_size,server.list_compress_depth);
        while(len--){
           if((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;  
           dec = getDecodedObject(ele);
           size_t len = sdslen(dec->ptr);
           quicklistPushTail(o->ptr, dec->ptr,len);
           decrRefCount(dec);
           decrRefCount(ele);
        };
    }else if(rdbtype == RDB_TYPE_SET){
        if((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL; 
        if(len > server.set_max_intset_entries){
            o = createSetObject(); 
            if(len > DICT_HT_INITIAL_SIZE){
                dictExpand(o->ptr,len); 
            }
        }else{
            o = createIntsetObject(); 
        }

        for(i = 0; i < len; i++){
            long long llval;
            sds sdsele;
            
            if((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) return NULL; 

            if(o->encoding == OBJ_ENCODING_INTSET){
                if(isSdsRepresentableAsLongLong(sdsele,&llval) == C_OK){
                    o->ptr = intsetAdd(o->ptr,llval,NULL); 
                }else{
                    setTypeConvert(o,OBJ_ENCODING_HT); 
                    dictExpand(o->ptr,len);
                } 
            };
            
           if(o->encoding == OBJ_ENCODING_HT){
                dictAdd((dict*)o->ptr,sdsele,NULL); 
           }else{
                sdsfree(sdsele); 
           } 
        };
    }else if(rdbtype == RDB_TYPE_ZSET_2 || rdbtype == RDB_TYPE_ZSET){
        uint64_t zsetlen; 
        size_t maxelelen = 0;
        zset *zs;
        
        if((zsetlen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL; 
        o = createZsetObject();
        zs = o->ptr;
        while(zsetlen--){
            sds sdsele; 
            double score;
            zskiplistNode *znode;

            if((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) return NULL;
            
            if(rdbtype == RDB_TYPE_ZSET_2){
                if(rdbLoadBinaryDoubleValue(rdb,&score) == -1) return NULL; 
            }else{
                if(rdbLoadDoubleValue(rdb,&score) == -1) return NULL; 
            }
            
            if(sdslen(sdsle) > maxelelen) maxelelen = sdslen(sdsele);
            znode = zslInsert(zs->zsl,score,sdsele);
            dictAdd(zs->dict,sdsele,&znode->score);
        }; 

        if(zsetLength(o) <= server.zset_max_ziplist_entries && maxelelen <= server.zset_max_ziplist_value){
            zsetConvert(o,OBJ_ENCODING_ZIPLIST); 
        
        }
    }else if(rdbtype == RDB_TYPE_HASH){
        uint64_t len;
        int ret;
        sds field, value; 

        len = rdbLoadLen(rdb,NULL);
        if(len == RDB_LENERR) return NULL;
        o = createHashObject();

        if(len > server.hash_max_ziplist_entries){
            hashTypeConvert(o,OBJ_ENCODING_HT); 
        }; 
        
        while(o->encoding == OBJ_ENCODING_ZIPLIST && len > 0){
            len--;
            if((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) return NULL; 
            if((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) return NULL;

            o->ptr = ziplistPush(o->ptr, (unsigned char *)field, sdslen(field), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, (unsigned char *)value, sdslen(value), ZIPLIST_TAIL);
            
            if(sdslen(field) > server.hash_max_ziplist_value || sdslen(value) > server.hash_max_ziplist_value){
           sdsfree(field); 
           sdsfree(value);
           hashTypeConvert(o,OBJ_ENCODING_HT); 
           break;
            
           } 

           sdsfree(field);
           sdsfree(value);
        };

        while(o->encoding == OBJ_ENCODING_HT && len > 0){
            len--; 

            if((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL){
                return NULL; 
            };
            if((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS)) == NULL){
                return NULL; 
            };
            
            ret = dictAdd((dict*)o->ptr,field,value); 
            if(ret == DICT_ERR){
                rdbExitReportCorruptRDB("Duplicate keys detected"); 
            }; 
        };
        
        serverAssert(len == 0);
       }else if(rdbtype == RDB_TYPE_LIST_QUICKLIST){
            if((len=rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;              
            o = createQuicklistObject();
            quicklistSetOptions(o->ptr, server.list_max_ziplist_size, server.list_compress_depth);
            while(len--){
                unsigned char *zl = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,NULL); 
                if(zl == NULL) return NULL;
                quicklistAppendZiplist(o->ptr,zl);
            }
       }else if( rdbtype == RDB_TYPE_HASH_ZIPMAP ||
                 rdbtype == RDB_TYPE_LIST_ZIPLIST ||
                 rdbtype == RDB_TYPE_SET_INTSET ||
                 rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
                 rdbtype == RDB_TYPE_HASH_ZIPLIST)
       {
            unsigned char *encoded = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,NULL); 
            if(encoded == NULL) return NULL;
            o = createObject(OBJ_STRING,encoded);
            
            switch(rdbtype){
                case RDB_TYPE_HASH_ZIPMAP:
                    {
                        unsigned char *zl = ziplistNew(); 
                        unsigned char *zi = zipmapRewind(o->ptr);
                        unsigned char *fstr, *vstr;
                        unsigned int flen, vlen;
                        unsigned int maxlen = 0;
                        
                        while((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL){
                            if(flen > maxlen) maxlen = flen; 
                            if(vlen > maxlen) maxlen = vlen;
                            zl = ziplistPush(zl,fstr,flen,ZIPLIST_TAIL);
                            zl = ziplistPush(zl,vstr,vlen,ZIPLIST_TAIL);
                        }; 

                        zfree(o->ptr);
                        o->ptr = zl;
                        o->type = OBJ_HASH;
                        o->encoding = OBJ_ENCODING_ZIPLIST;

                        if(hashTypeLength(o) > server.hash_max_ziplist_entries ||
                           maxlen > server.hash_max_ziplist_value     
                           ){
                           hashTypeConvert(o, OBJ_ENCODING_HT);  
                        };
                    } 
                    break;
               case RDB_TYPE_LIST_ZIPLIST:
                    o->type = OBJ_LIST;
                    o->encoding = OBJ_ENCODING_ZIPLIST;
                    listTypeConvert(o,OBJ_ENCODING_QUICKLIST);
                    break;
               case RDB_TYPE_SET_INTSET:
                   o->type = OBJ_SET;
                   o->encoding = OBJ_ENCODING_INTSET; 
                   if(intsetLen(o->ptr) > server.set_max_intset_entries){
                        setTypeConvert(o,OBJ_ENCODING_HT); 
                   }
                   break;
               case RDB_TYPE_ZSET_ZIPLIST:
                    o->type = OBJ_ZSET;
                    o->encoding = OBJ_ENCODING_ZIPLIST;
                    if(zsetLength(o) > server.zset_max_ziplist_entries){
                        zsetConvert(o,OBJ_ENCODING_SKIPLIST); 
                    };
                    break;
               case RDB_TYPE_HASH_ZIPLIST:
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_ZIPLIST;
                    if(hashTypeLength(o) > server.hash_max_ziplist_entries){
                        hashTypeConvert(o,OBJ_ENCODING_HT); 
                    };
                    break;
               default:
                    rdbExitReportCorruptRDB("Unknown RDB encoding type %d", rdbtype);
                    break; 
            };  
       }else if(rdbtype == RDB_TYPE_MODULE){
           uint64_t moduleid = rdbLoadLen(rdb,NULL);  
           moduleType *mt = moduleTypeLookupModuleByID(moduleid);
           char name[10];
           if(mt == NULL){
                moduleTypeNameByID(name,moduleid); 
                serverLog(LL_WARNING, "The RDB file contains module data I can't load: no matching module '%s'",name);
           exit(1);
           }
           RedisModuleIO io;
           moduleInitIOContext(io,mt,rdb);
           void *ptr = mt->rdb_load(&io,moduleid & 1023);
           if(ptr == NULL){
                moduleTypeNameByID(name,moduleid); 
                serverLog(LL_WARNING, "The RDB file contains module data for the module type '%s', that the responsible module is not able to load. Check for modules log above for additional clues",name);
                exit(1);
           }
           o = createModuleObject(mt,ptr);
       }else{
            rdbExitReportCorruptRDB("Unknown RDB encoding type %d", rdbtype); 
       };
        
       return o;
};

void startLoading(FILE *fp){
    struct stat sb;
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    if(fstat(fileno(fp),&sb) == -1){
        server.loading_total_bytes = 0; 
    }else{
        server.loading_total_bytes = sb.st_size; 
    }
};

void loadingProgress(off_t pos){
    server.loading_loaded_bytes = pos;
    if(server.stat_peak_memory < zmalloc_used_memory()){
        server.stat_peak_memory = zmalloc_used_memory(); 
    }
};

void stopLoading(void){
    server.loading = 0;
};


void rdbLoadProgressCallback(rio *r, const void *buf, size_t len){
   if(server.rdb_checksum){
        rioGenericUpdateChecksum(r,buf,len); 
   }; 

   if(server.loading_process_events_interval_bytes && 
          (r->processed_bytes + len) / server.loading_process_events_interval_bytes > 
         r->processed_bytes/server.loading_process_events_interval_bytes 
           ){
            updateCachedTime();
            if(server.masterhost && server.repl_state == REPL_STATE_TRANSFER){
                replicationSendNewlineToMaster(); 
            }; 
            loadingProgress(r->processed_bytes);
            processEventsWhileBlocked();
   }
};



int rdbLoadRio(rio *rdb, rdbSaveInfo *rsi){
    uint64_t dbid;
    int type, rdbver;
    redisDb *db = server.db + 0;
    char buf[1024];
    long long expiretime, now = mstime();
    rdb->update_cksum = rdbLoadProgressCallback;
    rdb->max_processing_chunk = server.loading_process_events_interval_bytes;
    
    if(rioRead(rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if(memcmp(buf,"REDIS",5) != 0){
        serverLog(LL_WARNING, "Wrong signature trying to load DB from file"); 
        errno = EINVAL;
        return C_ERR;
    };
    rdbver = atoi(buf + 5);
    if(rdbver < 1 || rdbver > RDB_VERSION){
        serverLog(LL_WARNING, "Can't handle RDB format version %d",rdbver); 
        errno = EINVAL;
        return C_ERR;
    };
    
    while(1){
        robj *key, *val;
        expiretime = -1; 

        if((type = rdbLoadType(rdb)) == -1) goto eoferr;
        if(type == RDB_OPCODE_EXPIRETIME){
            if((expiretime = rdbLoadTime(rdb)) == -1) got eoferr; 

            if((type = rdbLoadType(rdb)) == -1) goto eoferr;
            expiretime *= 1000;
        }else if(type == RDB_OPCODE_EXPIRETIME_MS){
            if((expiretime = rdbLoadMillisecondTime(rdb)) == -1) goto eoferr; 
            if((type = rdbLoadType(rdb)) == -1) goto eoferr;
        }else if(type == RDB_OPCODE_EOF){
            break;  
        }else if(type == RDB_OPCODE_SELECTDB){
            if((dbid = rdbLoadLen(rdb,NULL)) == RDB_LENERR){
                goto eoferr; 
            } 

            if(dbid >= (unsigned)server.dbnum){
                serverLog(LL_WARNING, "FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum); 
                exit(1); 
            };
            db= server.db + dbid; 
            continue;
        }else if(type == RDB_OPCODE_RESIZEDB){
            uint64_t db_size, expires_size;
            if((db_size == rdbLoadLen(rdb,NULL)) == RDB_LENERR){
                goto eoferr; 
            } 

            if((expires_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR){
                goto eoferr; 
            };

            dictExpand(db->dict,db_size);
            dictExpand(db->expires,expires_size);
            continue;
        }else if(type == RDB_OPCODE_AUX){
            robj *auxkey, *auxval;  
            if((auxkey = rdbLoadStringObject(rdb)) == NULL) goto eoferr;
            if((auxval = rdbLoadStringObject(rdb)) == NULL) goto eoferr;

            if(((char *)auxkey->ptr)[0] == "%"){
                serverLog(LL_NOTICE,"RDB '%s':%s", (char *)auxkey->ptr,(char*)auxval->ptr); 
            }else if(!strcasecmp(auxkey->ptr,"repl-stream-db")){
                if(rsi) rsi->repl_stream_db = atoi(auxval->ptr); 
            }else if(!strcasecmp(auxkey->ptr,"repl-id")){
                if(rsi && sdsle(auxval->ptr) == CONFIG_RUN_ID_SIZE){
                    memcpy(rsi->repl_id,auxval->ptr,CONFIG_RUN_ID_SIZE+1); 
                    rsi->repl_id_is_set = 1;
                } 
            }else if(!strcasecmp(auxkey->ptr,"repl-offset")){
                if(rsi) rsi->repl_offset = strtoll(auxval->ptr,NULL,10); 
            }else{
                serverLog(LL_DEBUG,"Unrecognized RDB AUX field: '%s'", (char *)auxkey->ptr); 
            }
        };

        decrRefCount(auxkey);
        decrRefCount(auxval);
        continue;
    }; 
    
    if((key = rdbLoadStringObject(rdb)) ==NULL) goto eoferr;
    if((value = rdbLoadObject(type,rdb)) == NULL) goto eoferr;


    if(server.masterhost == NULL && expiretime != -1 && expiretime < now){
        decrRefCount(key); 
        decrRefCount(val);
        continue;
    }

    dbAdd(db,key,val);
    if(expiretime != -1) setExpire(NULL,db,key,expiretime);
    decrRefCount(key);
    
    if(rdbver >= 5 && server.rdb_checksum){
        uint64_t cksum, expected = rdb->cksum; 
        if(rioRead(rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if(cksum == 0){
            serverLog(LL_WARNING,"RDB file was saved with checksum disabled: no check performed."); 
        }else if(cksum != expected){
            serverLog(LL_WARNING,"Wrong RDB checksum. Aborting now."); 
            rdbExitReportCorruptRDB("RDB CRC error");
        }
    }
    return C_OK;


eoferr:
    serverLog(LL_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbExitReportCorruptRDB("UNexpected EOF reading RDB file"); 
    return C_ERR;
};


int rdbLoad(char *filename, rdbSaveInfo *rsi){
    FILE *fp;
    rio rdb;
    int retval;


    if((fp = fopen(filename,"r")) == NULL) return C_ERR;
    startLoading(fp);
    rioInitWithFile(&rdb,fp);
    retval = rdbLoadRio(&rdb,rsi);
    fclose(p);
    stopLoading();
    return retval;
};


void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal){
    if(!bysignal && exitcode == 0){
        serverLog(LL_NOTICE, "Background saving terminated with success"); 
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = C_OK;
    }else if(!bysignal && exitcode != 0){
        serverLog(LL_WARNING, "Background saving error"); 
        server.lastbgsave_status = C_ERR;
    }else {
        mstime_t latency; 
        serverLog(LL_WARNING,"Background saving terminated by signal %d",bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(server.rdb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        if(bysignal != SIGUSR1){
            server.lastbgsave_status = C_ERR; 
        }
    }

    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = time(NULL) - server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_DISK);
};

void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal){
    uint64_t *ok_slaves;

    if(!bysignal && exitcode == 0){
        serverLog(LL_NOTICE, "Background RDB transfer terminated with success"); 
    }else if(!bysignal && exitcode != 0){
        serverLog(LL_WARNING, "Background transfer error");
    }else{
        serverLog(LL_WARNING, "Background transfer terminated by signal %d", bysignal); 
    };
    
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;

    ok_slaves = zmalloc(sizeof(uint64_t));  
    ok_slaves[0] = 0;
    if(!bysignal && exitcode == 0){
        int readlen = sizeof(uint64_t); 
        if(read(server.rdb_pipe_read_result_from_child, ok_slaves,readlen) == readlen){
            readlen = ok_slaves[0]*sizeof(uint64_t)*2; 
            ok_slaves = zrealloc(ok_slaves,sizeof(uint64_t)+readlen);
            if(readlen && read(server.rdb_pipe_read_result_from_child,ok_slaves+1,readlen) != readlen){
                ok_slaves[0] = 0; 
            };
        };
    };

    close(server.rdb_pipe_read_result_from_child);
    close(server.rdb_pipe_write_result_to_parent);

    listNode *ln;
    listIter li;
    listRewind(server.slaves, &li);
    
    while((ln = listNext(&li))){
       client *slave = ln->value;
       if(slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END){
            uint64_t j;
            int errorcode = 0; 

            for(j = 0; j < ok_slaves[0];j++){
                if(slave->id == ok_slaves[2*j + 1]){
                    errorcode = ok_slaves[2*j + 2]; 
                    break;
                }; 
            };

            if(j == ok_slaves[0] || errorcode != 0){
                serverLog(LL_WARNING, "Closing slave %s: child->slave RDB transfer failed: %s",
                                      replicationGetSlaveName(slave),
                                      (errorcode == 0) ? "RDB transfer child aborted" : strerror(errorcode)); 

             freeClient(slave);
            }else{
                serverLog(LL_WARNING,"Slave %s correctly received the streamed RDB file.", replicationGetSlaveName(slave));         
                anetNonBlock(NULL,slave->fd);
                anetSendTimeout(NULL,slave->fd,0);
            };
        };    
    }; 
    zfree(ok_slaves);
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_SOCKET);
};


void backgroundSaveDoneHandler(int exitcode, int bysignal){
    switch(server.rdb_child_type){
        case RDB_CHILD_TYPE_DISK:
           backgroundSaveDoneHandlerDisk(exitcode,bysignal); 
           break;
        case RDB_CHILD_TYPE_SOCKET:
           backgroundSaveDoneHandlerSocket(exitcode,bysignal);
           break;
        default:
           serverPanic("Unknown RDB child type.");
           break;
    };
};


int rdbSaveToSlavesSockets(rdbSaveInfo *rsi){
    int *fds;
    uint64_t *clientids;
    int numfds;
    listNode *ln;
    listIter li;
    pid_t childpid;
    long long start;
    int pipefds[2];

    if(server.aof_child_pid != -1 || server.rdb_child_pid != -1){
        return C_ERR; 
    }

    if(pipe(pipefds) == -1) return C_ERR;
    server.rdb_pipe_read_result_from_child = pipefds[0];
    server.rdb_pipe_write_result_to_parent = pipefds[1];

    fds = zmalloc(sizeof(int) * listLength(server.slaves));
    clientids = zmalloc(sizeof(uint64_t) * listLength(server.slaves));
    numfds = 0;
    
    listRewind(server.slaves,&li); 
    while((ln = listNext(&li))){
        client *slave = ln-value; 
        if(slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START){
            clientids[numfds] = slave->id; 
            fds[numfds++] = slave->fd;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
            anetBlock(NULL,slave->fd);
            anetSendTimeout(NULL,slave->fd,server.repl_timeout * 1000);
        }
    };
    
    openChildInfoPipe();
    start = ustime();
    if((childpid = fork())  == 0){
       int retval;
        rio slave_sockets;  
        rioInitWithFdset(&slave_sockets,fds,numfds);
        zfree(fds);
        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-to-slaves");
        retval = rdbSaveRioWithEOFMark(&slave_sockets,NULL,rsi);
        if(retval == C_OK && rioFlush(&slave_sockets) == 0){
            retval = C_ERR; 
        } 

        if(retval == C_OK){
            size_t private_dirty = zmalloc_get_private_dirty(-1); 
            if(private_dirty){
                serverLog(LL_NOTICE,"RDB: %zu MB of memory used by copy-on-write", private_dirty /(1024 * 1024)); 
            };
            server.child_info_data.cow_size = private_dirty;
            sendChildInfo(CHILD_INFO_TYPE_RDB);


            void *msg = zmalloc(sizeof(uint64_t) * (1 + 2*numfds));
            uint64_t *len = msg;
            uint64_t *ids = len + 1;
            int j, msglen;
            *len = numfds;
            for(j = 0; j < numfds; j++){
                *ids++ = clientids[j]; 
                *ids++ = slave_sockets.io.fdset.state[j];
            };
            
           msglen = sizeof(uint64_t) * (1+2*numfds);
           if(*len == 0 || write(server.rdb_pipe_write_result_to_parent,msg,msglen) != msglen){
               retval = C_ERR;  
           }
           zfree(msg);
        }

        zfree(clientids);
        rioFreeFdset(&slave_sockets);
        exitFromChild((retval == C_OK) ? 0 : 1);
    }else{
        server.stat_fork_time = ustime() - start; 
        server.stat_fork_rate = (double)zmalloc_used_memory() * 1000000 / server.stat_fork_time/(1024 * 1024*1024);
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if(childpid == -1){
            serverLog(LL_WARNING, "Can't save in background: fork:%s",strerror(errno)); 
            listRewind(server.slaves,&li);
            while((ln = listNext(&li))){
                client *slave = ln->value;
                int j; 

                for(j = 0; j < numfds; j++){
                    if(slave->id == clientids[j]){
                        slave->replstate = SLAVE_STATE_WAIT_BGSAVE_START; 
                        break;
                    }; 
                };
            };
            close(pipefds[0]);
            close(pipefds[1]);
            closeChildInfoPipe();
        }else{
            serverLog(LL_NOTICE,"Background RDB transfer stated by pid %d", childpid); 
            server.rdb_save_time_start = time(NULL);
            server.rdb_child_pid = childpid;
            server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;
            updateDictResizePolicy();
        };
        zfree(clientids);
        zfree(fds);
        return (childpid == -1) ? C_ERR : C_OK;
    };
    
    return C_OK;
};


void saveCommand(client *c){
    if(server.rdb_child_pid != -1){
        addReplyError(c,"Background save already in progress"); 
        return;
    };

    if(rdbSave(server.rdb_filename,NULL) == C_OK){
        addReply(c,shared.ok); 
    }else{
        addReply(c,shared.err); 
    }
};



void bgsaveCommand(client *c){
    int schedule = 0;

    if(c->argc > 1){
        if(c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"schedule")){
            schedule = 1; 
        }else{
            addReply(c,shared.syntaxerr); 
            return;
        } 
    }


    if(server.rdb_child_pid != -1){
        addReplyError(c,"Background save already in progress"); 
    }else if(server.aof_child_pid != -1){
        if(schedule){
            server.rdb_bgsave_scheduled = 1; 
            addReplyStatus(c,"Background saving scheduled");
        }else{
            addReplyError(c,"An AOF log rewriting in progress: can't BGSAVE right now. Use BGSAVE SCHEDULE in order to scehdule a BGSABVE whenever possible"); 
        } 
    }else if(rdbSaveBackground(server.rdb_filename, NULL) == C_OK){
        addReplyStatus(c,"Background saving started"); 
    }else{
        addReply(c,shared.err); 
    }
};









