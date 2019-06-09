#include "server.h"
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>

void aofUpdateCurrentSize(void);
void aofClosePipes(void);

#define AOF_RW_BUF_BLOCK_SIZE (1024 * 1024 * 10)

typedef struct aofrwblock
{
    unsigned long used, free;
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

void aofRewriteBufferReset(void)
{
    if (server.aof_rewrite_buf_blocks)
    {
        listRelease(server.aof_rewrite_buf_blocks);
    };

    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks, zfree);
};

unsigned long aofRewriteBufferSize(void)
{
    listNode *ln;
    listIter li;
    unsigned long size = 0;

    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li)))
    {
        aofrwblock *block = listNodeValue(ln);
        size += block->used;
    };
    return size;
};



void aofChildWriteDiffData(aeEventLoop *el, int fd, void *privdata, int mask)
{
    listNode *ln;
    aofrwblock *block;
    ssize_t nwritten;

    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);

    while (1)
    {
        ln = listFirst(server.aof_rewrite_buf_blocks);
        block = ln ? ln->value : NULL;
        if (server.aof_stop_sending_diff || !block)
        {
            aeDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE);
            return;
        };

        if (block->used > 0)
        {
            nwritten = write(server.aof_pipe_write_data_to_child, block->buf, block->used);
            if (nwritten <= 0)
                return;
            memmove(block->buf, block->buf + nwritten, block->used - nwritten);
            block->used -= nwritten;
        };

        if (block->used == 0)
            listDelNode(server.aof_rewrite_buf_blocks, ln);
    };
};

void aofRewriteBufferAppend(unsigned char *s, unsigned long len)
{
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    while (len)
    {
        if (block)
        {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen)
            {
                memcpy(block->buf + block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            };
        }

        if (len)
        {
            int numblocks;

            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            listAddNodeTail(server.aof_rewrite_buf_blocks, block);

            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks + 1) % 10) == 0)
            {
                int level = ((numblocks + 1) % 100) == 0 ? LL_WARNING : LL_NOTICE;
                serverLog(level, "Background AOF buffer size: %lu MB", aofRewriteBufferSize() / (1024 * 1024));
            };
        };
    };

    if (aeGetFileEvents(server.el, server.aof_pipe_write_data_to_child) == 0)
    {
        aeCreateFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE, aofChildWriteDiffData, NULL);
    }
};

ssize_t aofRewriteBufferWrite(int fd)
{
    listNode *ln;
    listIter li;

    ssize_t count = 0;

    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li)))
    {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;
        if (block->used)
        {
            nwritten = write(fd, block->buf, block->used);
            if (nwritten != (ssize_t)block->used)
            {
                if (nwritten == 0)
                    errno = EIO;
                return -1;
            };
            count += nwritten;
        };
    };

    return count;
};

void aof_background_fsync(int fd)
{
    bioCreateBackgroundJob(BIO_AOF_FSYNC, (void *)(long)fd, NULL, NULL);
};

void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc){
    sds buf = sdsempty();
    robj *tamargv[3];

    if(dictid != server.aof_selected_db){
        char seldb[64];
        snprintf(seldb, sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf, "*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",(unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    };

    

};


void stopAppendOnly(void)
{
    serverAssert(server.aof_state != AOF_OFF);
    flushAppendOnlyFile(1);
    aof_fsync(server.aof_fd);
    close(server.aof_fd);

    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = AOF_OFF;

    if (server.aof_child_pid != -1)
    {
        int statloc;

        serverLog(LL_NOTICE, "Killing running AOF rewrite child: %ld", (long)server.aof_child_pid);
        if (kill(server.aof_child_pid, SIGUSR1) != -1)
        {
            while (wait3(&statloc, 0, NULL) != server.aof_child_pid)
                ;
        };
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
        aofClosePipes();
    };
};

int startAppendOnly(void)
{
    char cwd[MAXPATHLEN];
    server.aof_last_fsync = server.unixtime;
    server.aof_fd = open(server.aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    serverAssert(server.aof_state == AOF_OFF);
    if (server.aof_fd == -1)
    {
        char *cwdp = getcwd(cwd, MAXPATHLEN);
        serverLog(LL_WARNING,
                  "Redis needs to enable the AOF but can't open the append only file %s (in server root dir %s): %s", server.aof_filename, cwdp ? cwdp : "unknown", strerror(errno));
        return C_ERR;
    };

    if (server.rdb_child_pid != -1)
    {
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_WARNING, "Redis was enabled but there is already a child process saving an RDB file on disk. An AOF backgound was scheduled to start when possible.");
    }
    else if (rewriteAppendOnlyFileBackground() == C_ERR)
    {
        close(server.aof_fd);
        serverLog(LL_WARNING, "Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return C_ERR;
    }

    server.aof_state = AOF_WAIT_REWRITE;
    return C_OK;
};

#define AOF_WRITE_LOG_ERROR_RATE 30
void flushAppendOnlyFile(int force)
{
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;

    if (sdslen(server.aof_buf) == 0)
        return;

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
    {
        sync_in_progress = bioPendingJobsOfType(BIO_AOF_FSYNC) != 0;
    };

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force)
    {
        if (sync_in_progress)
        {
            if (server.aof_flush_postponed_start == 0)
            {
                server.aof_flush_postponed_start = server.unixtime;
                return;
            }
            else if (server.unixtime - server.aof_flush_postponed_start < 2)
            {
                return;
            }
        };

        server.aof_delayed_fsync++;
        serverLog(LL_NOTICE, "Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
    };

    latencyStartMonitor(latency);
    nwritten = write(server.aof_fd, server.aof_buf, sdslen(server.aof_buf));
    latencyEndMonitor(latency);

    if (sync_in_progress)
    {
        latencyAddSampleIfNeeded("aof-write-pending-fsync", latency)
    }
    else if (server.aof_child_pid != -1 || server.rdb_child_pid != -1)
    {
        latencyAddSampleIfNeeded("aof-write-active-child", latency);
    }
    else
    {
        latencyAddSampleIfNeeded("aof-write-alone", latency);
    }

    latencyAddSampleIfNeeded("aof-write", latency);

    server.aof_flush_postponed_start = 0;
    if (nwritten != (signed)sdslen(server.aof_buf))
    {
        static time_t last_write_error_log = 0;
        int can_log = 0;

        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE)
        {
            can_log = 1;
            last_write_error_log = server.unixtime;
        };

        if (nwritten == -1)
        {
            if (can_log)
            {
                serverLog(LL_WARNING, "Error writing to the AOF file: %s", strerror(errno));
                server.aof_last_write_errno = errno;
            };
        }
        else
        {
            if (can_log)
            {
                serverLog(LL_WARNING, "Short write while writing to the AOF file: (nwritten=%lld, expected=%lld)", (long long)nwritten, (long long)sdslen(server.aof_buf));
            };

            if (ftruncate(server.aof_fd, server.aof_current_size) == -1)
            {
                if (can_log)
                {
                    serverLog(LL_WARNING, "Could not remove short write from the append-only file. Redis may refuse to load the AOF the next time it starts. ftruncate: %s", strerror(errno));
                };
            }
            else
            {
                nwritten = -1;
            }

            server.aof_last_write_errno = ENOSPC;
        };

        if(server.aof_fsync == AOF_FSYNC_ALWAYS){
            serverLog(LL_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        }else{
            server.aof_last_write_status = C_ERR;

            if(nwritten > 0){
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten , -1);
            };
            return;
        };

    }else{
        if(server.aof_last_write_status == C_ERR){
            serverLog(LL_WARNING, "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = C_OK;
        }
    };
    server.aof_current_size += nwritten;

    if((sdslen(server.aof_buf) + sdsavail(server.aof_buf)) < 4000){
        sdsclear(server.aof_buf);
    }else{
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    };

    if(server.aof_no_fsync_on_rewrite && (server.aof_child_pid != -1 || server.rdb_child_pid != -1)){
        return;
    };

    if(server.aof_fsync == AOF_FSYNC_ALWAYS){
        latencyStartMonitor(latency);
        aof_fsync(server.aof_fd);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always",latency);
        server.aof_last_fsync == server.unixtime;
    }else if((server.aof_fsync == AOF_FSYNC_EVERYSEC && server.unixtime > server.aof_last_fsync)){
        if(!sync_in_progress) aof_background_fsync(server.aof_fd);
        server.aof_last_fsync = server.unixtime;
    };
};

sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv){
    char buf[32];
    int len, j;
    robj *o;

    buf[0] =  '*';
    len = 1 + ll2string(buf+1, sizeof(buf)-1,argc);

    buf[len++] = '\r';
    buf[len++] = '\n';

    dst = sdscatlen(dst,buf,len);

    for(j =0; j < argc; j++){
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';

        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        decrRefCount(o);
    };

    return dst;
}


