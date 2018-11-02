#ifndef LZFP_h
#define LZFP_h


#define STANDALONE 1
#ifndef STANDALONE 
# include "lzf.h"
#endif

#ifndef HLOG
# define HLOG 16
#endif



#ifndef VERY_FAST
# define VERY_FAST 1
#endif


#ifndef ULTRA_FAST
# define ULTRA_FAST 0
#endif


#ifndef STRICT_ALIGN
# define STRICT_ALIGN !(defined(__i386) || defined (__amd64))
#endif


#ifndef INT_HTAB
# define INIT_HTAB 0
#endif


#ifndef LZF_STATE_ARG
# define LZF_STATE_ARG 0
#endif

#ifndef CHECK_INPUT
# define CHECK_INPUT 1
#endif


#ifdef __cplusplus
# include <cstring>
# include <climits>
using namespace std;
#else
# include <string.h>
# include <limits.h>
#endif


#ifndef LZF_USE_OFFSETS
# if defined (WIN32)
#  define LZF_USE_OFFSETS defined(_M_X64)
# else
#  if __cplusplus > 199711L
#   include <stdint>
#  else
#   include <stdint.h>
#  endif
#  define LZF_USE_OFFSETS (UINTPTR_MAX > 0xffffffffU)
# endif
#endif


typedef unsigned char u8;


#if LZF_USE_OFFSETS
# define LZF_HSLOT_BIAS ((const u8 *)in_data)
  typedef unsigned int LZF_HSLOT;
#else
#define LZF_HSLOT_BIAS 0
 typedef const u8 * LZF_HSLOT;
#endif

typedef LZF_HSLOT LZF_STATE[1 << (HLOG)];

#if !STRICT_ALIGN
#if USHRT_MAX == 65535
    typedef unsigned short u16;
#elif UNIT_MAX == 65535
    typedef unsigned int u16;
#else
# undef STRICT_ALIGN
# define STRICT_ALIGN 1
#endif
#endif


#if ULTRA_FAST
# undef VERY_FAST
#endif

#endif


