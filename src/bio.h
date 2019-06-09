void bioInit(void);
void bioCreateBackgroundJob(int type, void *arg1, void *arg2,void *arg3);
unsigned long long bioPendingJobsOfType(int type);
unsigned long long bioWaitStepOfType(int type);
time_t bioOlderJobOfType(int type);
void bioKillThreads(void);


#define BIO_CLOSE_FILE 0
#define BIO_AOF_FSYNC 1
#define BIO_LAZY_FREE 2
#define BIO_NUM_OPS 3
