#include "lzfP.h"

#define HSIZE (1 << (HLOG))

#ifndef FRST
# define FRST(p) ((p[0]) << 8 | p[1])
# define NEXT(v,p) (((v) << 8) | p[2])
# if ULTRA_FAST
#  define IDX(h) (((h               >> (3*8 - HLOG)) -h) & (HSIZE - 1))
# elif VERY_FAST
#  define IDX(h) (((h               >> (3*8 - HLOG)) -h) & (HSIZE - 1))
# else
#  define IDX(h) ((((h ^ (h << 5))  >> (3*8 - HLOG)) -h) & (HSIZE - 1))
# endif
#endif


#if 0
# define FRST(p) (p[0] << 5) ^ p[1]
# define NEXT(v,p) ((v) << 5) ^ p[2]
# define IDX(h) ((h) & (HSIZE - 1)) 
#endif


#define MAX_LIT (1 << 5)
#define MAX_OFF (1 << 13)
#define MAX_REF ((1 << 8) + (1 << 3))

#if __GNUC__ >= 3
# define expect(expr, value)       __builtin_expect ((expr),(value))
# define inline                    inline
#else
# define expect(expr,value)        (expr)
# define inline                    static
#endif


#define expect_false(expr) expect ((expr) != 0, 0)
#define expect_true(expr) expect ((expr) != 0, 1)

unsigned int lzf_compress(const void *const in_data, unsigned int in_len, void *out_data, unsigned int out_len
#if LZF_STATE_ARG
        , LZF_STATE htab
#endif
){
#if !LZF_STATE_ARG
    LZF_STATE htab;
#endif
    const u8 *ip = (const u8 *)in_data;
          u8 *op = (u8 *)out_data;
    const u8 *in_end = ip + in_len;
          u8 *out_end = op + out_len;
    const u8 *ref;

#if defined (WIN32) && defined (_M_X64)
    unsigned _int64 off;
#else
    unsigned long off;
#endif
    unsigned int hval;
    int lit;
    
    if(!in_len || !out_len){
        return 0;
    }

#if INIT_HTAB
    memset(htab, 0, sizeof (htab));
#endif
    lit = 0; op++;
    
    hval = FRST(ip);
    while(ip < in_end -2){
        LZF_HSLOT *hslot;
        hval = NEXT(hval,ip);
        hslot = htab + IDX(hval);
        ref = *hslot + LZF_HSLOT_BIAS;
        *hslot = ip - LZF_HSLOT_BIAS;
        
        if(1
#if INIT_HTAB
        && ref < ip
#endif
        && (off = ip - ref - 1) < MAX_OFF
        && ref > (u8 *) in_data
        && ref[2] == ip[2]
#if STRICT_ALIGN
        && ((ref[1] << 8) | ref[0]) == ((ip[1] << 8) | ip[0])
#else
        && *(u16 *)ref == *(u16 *)ip
#endif
        ){
            unsigned int len = 2;
            unsigned int maxlen = in_end - ip - len;
            maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;
            
            
            if(expect_false (op + 3 + 1 >= out_end)){
                if(op - !lit + 3 + 1 >= out_end){
                    return 0;
                }
            };
            
            op [- lit - 1] = lit - 1;
            op -= !lit;
            
            for(;;){
                if(expect_true (maxlen > 16)){
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;

                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                    len++; if(ref [len] != ip [len]) break;
                }
                do{
                    len++;
                }while(len < maxlen && ref[len] == ip[len]);
                
                break;
            };
            
            len -= 2;
            ip++;
            
            if(len < 7){
                *op++ = (off >> 8) + (len << 5);
            }else{
                *op++ = (off >> 8) + ( 7 << 5);
                *op++ = len - 7;
            }
            
            *op++ = off;
            lit = 0; op++;
            ip += len + 1;
            if(expect_false (ip >= in_end - 2)){break;};

#if ULTRA_FAST || VERY_FAST
            --ip;
# if VERY_FAST && !ULTRA_FAST
            --ip;
#endif
            hval = FRST(ip);
            
            hval = NEXT(hval, ip);
            htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
            ip++;
# if VERY_FAST && !ULTRA_FAST
            hval = NEXT (hval,ip);
            htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
            ip++;
#endif
#else       
            ip -= len + 1;
            do{
                hval = NEXT(hval,ip);
                htab[IDX (hval)] = ip - LZF_HSLOT_BIAS;
                ip++;
            }while(len--);
#endif
       }else{
            if(expect_false (op >= out_end)){
                return 0; 
            }
            
            lit++; *op++ = *ip++;
            if(expect_false (lit == MAX_LIT)){
                op [- lit - 1] = lit - 1;
                lit = 0;
                op++;
            }
        } 
    };
    if(op + 3 > out_end){return 0;};
    
    while(ip < in_end){
        lit++;
        *op++ = *ip++;
        if(expect_false (lit == MAX_LIT)){
            op [- lit - 1] = lit - 1;
            lit = 0; op++;
        }
    };
    
    op [- lit - 1] = lit - 1;
    op -= !lit;
    return op - (u8 *)out_data;

}

