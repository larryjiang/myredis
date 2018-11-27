#ifndef __REDIS_H
#define __REDIS_H

#include "fmacros.h"
#include "config.h"
#include "solarisfixes.h"
#include "rio.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <lua.h>
#include <signal.h>

//milisecond time type
typedef long long mstime_t;

#include "ae.h"
#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "zmalloc.h"
#include "anet.h"
#include "ziplist.h"
#include "intset.h"
#include "version.h"
#include "util.h"
#include "latency.h"
#include "sparkline.h"
#include "quicklist.h"

#include "rax.h"

#include "zipmap.h"
#include "sha1.h"
#include "endianconv.h"
#include "crc64.h"

#define C_OK   0
#define C_ERR  -1



#endif
