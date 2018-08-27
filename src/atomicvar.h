#include <pthread.h>

#ifndef __ATOMIC_VAR_H
#define __ATOMIC_VAR_H

#if defined(__ATOMIC_RELAXED) && !defined(__sun) && (!defined(__clang__) || !defined(__APPLE__) || __apple_build_version__ > 4210057)


#define atomicIncr(var,count,mutex) __atomic_add_fetch(&var,(count),__ATOMIC_RELAXED)
#define atomicDecr(var,count,mutex) __atomic_sub_fetch(&var,(count),__ATOMIC_RELAXED)

#define atomicGet(var,dstvar,mutex) do {\
    dstvar = __atomic_load_n(&var,__ATOMIC_RELAXED);\
}while(0)


#elif defined(HAVE_ATOMIC)
#define atomicIncr(var,count,mutex) __sync_add_and_fetch(&var,(count))
#define atomicDecr(var,count,mutex) __sync_sub_and_fetch(&var,(count))
#define atomicGet(var,count,mutex) do{ \
    dstvar = __sync_sub_and_fetch(&var,0);\
}while(0)

#else

#define atomicIncr(var,count,mutex) do {\
    pthread_mutex_lock(&mutex);\
    var += (count); \
    pthread_mutex_unlock(&mutex);\
}while(0)

#define atomicDecr(var,count,mutex) do {\
    pthread_mutex_lock(&mutex);\
    var -= (count);\
    pthread_mutex_unlock(&mutex);\
}while(0)


#define atomicGet(var,dstvar,mutex) do{\
    pthread_mutex_lock(&mutex);\
    dstvar = var;\
    pthread_mutex_unlock(&mutex);\
}while(0)
#endif

#endif

 
