#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"


#define ZIPMAP_BIGLEN 254
#define ZIPMAP_END 255



#define ZIPMAP_VALUE_MAX_FREE 4 
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int) + 1)


unsigned char *zipmapNew(void){
    unsigned char *zm = zmalloc(2);
    zm[0] = 0;
    zm[1] = ZIPMAP_END; 
    return zm;
};

static unsigned int zipmapDecodeLength(unsigned char *p){
    unsigned int len = *p;

    if(len < ZIPMAP_BIGLEN) return len;
    memcpy(&len, p+1,sizeof(unsigned int));
    memrev32ifbe(&len);
    return len;
};

static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len){
    if(p == NULL){
        return ZIPMAP_LEN_BYTES(len); 
    }else{
        if(len < ZIPMAP_BIGLEN){
            p[0] = len;
            return 1; 
        }else{
            p[0] = ZIPMAP_BIGLEN; 
            memcmp(p + 1, &len, sizeof(len)); 
            memrev32ifbe(p+1);
            return 1 + sizeof(len);
        } 
    }
};


static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen){
    unsigned char *p = zm + 1, *k = NULL;
    unsigned int l, llen;

    while(*p != ZIPMAP_END){
        unsigned char free;
        l = zipmapDecodeLength(p); 
        llen = zipmapEncodeLength(NULL,l);
        if(key != NULL && k == NULL && l == klen && !memcmp(p + llen, key, l)){
            if(totlen != NULL){
                k = p; 
            }else{
                return p; 
            } 
        }
        p += llen + l;
        
        l = zipmapDecodeLength(p); 
        p += zipmapEncodeLength(NULL,l);
        free= p[0];
        p += l + 1 + free;
    };
    
    if(totlen != NULL) *totlen = (unsigned int)(p-zm) + 1;
    return k;
};


static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen){
    unsigned int l;
    l = klen + vlen + 3;
    if(klen >= ZIPMAP_BIGLEN) l += 4;
    if(vlen >= ZIPMAP_BIGLEN) l += 4;
    return l;
};


static unsigned int zipmapRawKeyLength(unsigned char *p){
    unsigned int l = zipmapDecodeLength(p);
    return zipmapEncodeLength(NULL,l) + l;
};

static unsigned int zipmapRawValueLength(unsigned char *p){
    unsigned int l = zipmapDecodeLength(p);
    unsigned int used;

    used = zipmapEncodeLength(NULL,l);
    used += p[used] + 1 + l;
    return used;
}; 

static unsigned int zipmapRawEntryLength(unsigned char *p){
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p + l);
};

static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len){
    zm = zrealloc(zm, len);
    zm[len - 1] = ZIPMAP_END;
    return zm;
};


unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update){
    unsigned int zmlen, offset;
    unsigned int freelen, reqlen = zipmapRequiredLength(klen,vlen);
    unsigned int empty, vempty;
    unsigned char *p;
    freelen = reqlen;
    if(update) *update = 0;
    p = zipmapLookupRaw(zm,key,klen,&zmlen); 
    if(p == NULL){
        zm = zipmapResize(zm, zmlen + reqlen); 
        p = zm + zmlen - 1;
        zmlen = zmlen + reqlen;
        if(zm[0] < ZIPMAP_BIGLEN) zm[0]++;
    }else{
        if(update) *update = 1;
        freelen = zipmapRawEntryLength(p); 
        if(freelen < reqlen){
            offset = p - zm;
            zm = zipmapResize(zm,zmlen - freelen + reqlen);  
            p = zm + offset;
            memmove(p+reqlen, p + freelen, zmlen - (offset + freelen + 1));
            zmlen = zmlen - freelen + reqlen;
            freelen = reqlen;
        };
    }

    empty = freelen - reqlen;
    if(empty >= ZIPMAP_VALUE_MAX_FREE){
        offset = p - zm;
        memmove(p + reqlen, p + freelen, zmlen - (offset + freelen + 1)); 
        zmlen -= empty;
        zm = zipmapResize(zm,zmlen);
        p = zm + offset;
        vempty = 0;
    }else{
        vempty = empty; 
    }; 

    p += zipmapEncodeLength(p,klen);
    memcpy(p,key,klen);
    p += klen;

    p += zipmapEncodeLength(p,vlen);
    *p++ = vempty;
    memcpy(p,val,vlen);
    return zm;
};

unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted){
    unsigned int zmlen , freelen;
    unsigned char *p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if(p){
        freelen = zipmapRawEntryLength(p); 
        memmove(p,p+freelen,zmlen - ((p - zm) + freelen + 1));
        if(zm[0] < ZIPMAP_BIGLEN) zm[0]--;
        if(deleted) *deleted = 1;
    }else{
        if(deleted) *deleted = 0; 
    }
    return zm;
};

unsigned char *zipmapRewind(unsigned char *zm){
    return zm + 1;
}


unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen){
   if(zm[0] == ZIPMAP_END) return NULL; 
   if(key){
        *key = zm;
        *klen = zipmapDecodeLength(zm);  
        *key += ZIPMAP_LEN_BYTES(*klen);
   }
   zm += zipmapRawKeyLength(zm);
   if(value){
        *value = zm + 1;
        *vlen = zipmapDecodeLength(zm); 
        *value += ZIPMAP_LEN_BYTES(*vlen);
   }
   zm += zipmapRawValueLength(zm);
   return zm;
};


int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen){
    unsigned char *p;
    if((p = zipmapLookupRaw(zm,key,klen,NULL)) == NULL) return 0;
    p += zipmapRawKeyLength(p);
    *vlen = zipmapDecodeLength(p);
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1;
    return 1;
};


int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen){
    return zipmapLookupRaw(zm,key,klen,NULL) != NULL;
}


unsigned int zipmapLen(unsigned char *zm){
    unsigned int len = 0;
    if(zm[0] < ZIPMAP_BIGLEN){
        len = zm[0]; 
    }else{
        unsigned char *p = zipmapRewind(zm); 
        while((p = zipmapNext(p,NULL,NULL,NULL,NULL)) != NULL) len++;
        if(len < ZIPMAP_BIGLEN) zm[0] = len;
    }
    return len;
}

size_t zipmapBlobLen(unsigned char *zm){
    unsigned int totlen;
    zipmapLookupRaw(zm,NULL,0,&totlen);
    return totlen;
};

#ifdef REDIS_TEST
static void zipmapRepr(unsigned char *p){
    unsigned int l;
    printf("{status %u}",*p++);
    while(1){
        if(p[0] == ZIPMAP_END){
            printf("{end}"); 
            break;
        }else{
            unsigned char e;
            l = zipmapDecodeLength(p); 
            printf("{key %u}",l);
            p += zipmapEncodeLength(NULL,l);
            if(l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l;
            l = zipmapDecodeLength(p);
            printf("{value %u}",l);
            p += zipmapEncodeLength(NULL,l);
            e = *p++;
            if(l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l + e;
            if(e){
                printf("["); 
                while(e--) printf(".");
                printf("]");
            }
        } 
    }
    printf("\n");
};


#define UNUSED(x) (void)(x)
int zipmapTest(int argc, char *argv[]){
    unsigned char *zm;
    UNUSED(argc);
    UNUSED(argv);


    zm = zipmapNew();
    zm = zipmapSet(zm, (unsigned char*) "name", 4, (unsigned char *) "foo", 3, NULL);
    zm = zipmapSet(zm, (unsigned char*) "surname",7,(unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm, (unsigned char*) "age",3,(unsigned char*) "foo",3,NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm, (unsigned char*) "hello", 5, (unsigned char*) "world!",6,NULL);
    zm = zipmapSet(zm, (unsigned char*) "foo", 3, (unsigned char*) "bar", 3, NULL);
    zm = zipmapSet(zm, (unsigned char*) "foo",3,(unsigned char*) "!",1,NULL)
    zipmapRepr(zm);
    zm = zipmapSet(zm, (unsigned char*) "foo",3,(unsigned char*)"12345",5,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm, (unsigned char*) "new",3,(unsigned char*)"xx",2,NULL);
    zm = zipmapSet(zm, (unsigned char*) "noval",5,(unsigned char*)"",0,NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm, (unsigned char*) "new",3,"NULL");
    zipmapRepr(zm);

    printf("\n Look up large key:\n");
    {
        unsigned char buf[512]; 
        unsigned char *value;
        unsigned int vlen, i;
        for(i = 0; i< 512; i++) buf[i] = 'a';
        
        zm = zipmapSet(zm, buf, 512,(unsigned char*) "long", 4,NULL); 
        if(zipmapGet(zm,buf,512,&value, &vlen)){
            printf(" <long key> is assocaited to the %d bytes value: %.*s\n", vlen,vlen,value); 
        }
    }

    printf("\n Perform a direct lookup: \n");
    {
        unsigned char *value;
        unsigned int vlen;

        if(zipmapGet(zm, (unsigned char*) "foo", 3, &value, &vlen)){
            printf("  foo isassocated to the %d bytes value: %.*s\n", vlen, vlen, value); 
        }; 
    }

    printf("\nIterate through elements: \n");
    {
        unsigned char *i = zipmapRewind(zm); 
        unsigned char *key, *value;
        unsigned int klen, vlen;
        while((i = zipmapNext(i, &key, &klen, &value,&vlen)) != NULL){
            printf("  %d:%.*s => %d:%.*s\n",klen, klen,key,vlen,vlen,value); 
        }  
    }
    return 0;
};
#endif








