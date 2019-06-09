#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128


typedef struct slowlogEntry {
    robj **argv;
    int argc;
    long long id;
    long long duration;
    time_t time;
} slowlogEntry;



void slowlogInit(void);
void slowlogPushEntryIfNeeded(robj **argv, int argc, long long duration);


void slowlogCommand(client *c);
