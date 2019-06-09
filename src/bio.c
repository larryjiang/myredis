#include "server.h"
#include "bio.h"

static pthread_t bio_threads[BIO_NUM_OPS];
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS];
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];

static list *bio_jobs[BIO_NUM_OPS];

static unsigned long long bio_pending[BIO_NUM_OPS];

struct bio_job{
    time_t time;

    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);
void lazyfreeFreeObjectFromBioThread(robj *o);
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2);
void lazyfreeFreeSlotsMapFromBioThread(zskiplist *sl);

#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

void bioInit(void){
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    for(j = 0; j < BIO_NUM_OPS; j++){
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_newjob_cond[j],NULL);
        pthread_cond_init(&bio_step_cond[j],NULL);
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    };

    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr,&stacksize);

    if(!stacksize) stacksize = 1;

    while(stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr,stacksize);

    for(j = 0; j < BIO_NUM_OPS;j++){
        void *arg = (void*)(unsigned long) j;
        if(pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0){
            serverLog(LL_WARNING,"Fatal: Can't initialzie Background Jobs.");
            exit(1);
        };
        bio_threads[j]  = thread;
    };
};


void *bioProcessBackgroundJobs(void *arg){
    struct bio_job *job;
    unsigned long type = (unsigned long) arg;


    sigset_t sigset;

    if(type >= BIO_NUM_OPS){
        serverLog(LL_WARNING,"Warning: bio thread started with wrong type %lu",type);
        return NULL;
    };

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,NULL);

    pthread_mutex_lock(&bio_mutex[type]);
    sigemptyset(&sigset);
    sigaddset(&sigset,SIGALRM);

    if(pthread_sigmask(SIG_BLOCK, &sigset, NULL)){
        serverLog(LL_WARNING,"Warning: can't mask SIGALARM in bio.c thread: %s", strerror(errno));
    };

    while(1){
        listNode *ln;
        if(listLength(bio_jobs[type]) == 0){
            pthread_cond_wait(&bio_newjob_cond[type],&bio_mutex[type]);
            continue;
        };

        ln = listFirst(bio_jobs[type]);
        job = ln->value;

        pthread_mutex_unlock(&bio_mutex[type]);

        if(type == BIO_CLOSE_FILE){
            close((long) job->arg1);
        }else if(type == BIO_AOF_FSYNC){
            aof_fsync((long)job->arg1);
        }else if(type == BIO_LAZY_FREE){
            if(job->arg1){
                lazyfreeFreeObjectFromBioThread(job->arg1);
            }else if(job->arg2 && job->arg3){
                lazyfreeFreeDatabaseFromBioThread(job->arg2,job->arg3);
            }else if(job->arg3){
                lazyfreeFreeSlotsMapFromBioThread(job->arg3);
            }
        }else{
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }

        zfree(job);
        pthread_cond_broadcast(&bio_step_cond[type]);

        pthread_mutex_lock(&bio_mutex[type]);
        listDelNode(bio_jobs[type],ln);
        bio_pending[type]--;
    };
};

void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3){
    struct bio_job *job = zmalloc(sizeof(*job));
     
    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;

    pthread_mutex_lock(&bio_mutex[type]);
    listAddNodeTail(bio_jobs[type],job);
    bio_pending[type]++;
    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
};


unsigned long long bioPendingJobsOfType(int type){
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
};

unsigned long long bioWaitStepOfType(int type){
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    if(val != 0){
        pthread_cond_wait(&bio_step_cond[type],&bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
};

void bioKillThreads(void){
    int err, j;

    for(j = 0; j < BIO_NUM_OPS;j++){
        if(pthread_cancel(bio_threads[j]) == 0){
            if((err = pthread_join(bio_threads[j],NULL)) != 0){
                serverLog(LL_WARNING, "Bio thread for job type #%d can be joined: %s",j, strerror(err));
            }else{
                serverLog(LL_WARNING,"Bio thread for job type #%d terminated",j);
            }
        };
    };
};







