#ifndef RAX_ALLOC_H
#define RAX_ALLOC_H
#include "zmalloc.h"
#define rax_malloc zmalloc
#define rax_realloc zrealloc
#define rax_free zfree
#endif

