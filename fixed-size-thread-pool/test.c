#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#include "tpool.h"

static const size_t num_threads = 4;
static const size_t num_items   = 100;

void worker(void *arg)
{
    int *val = arg;
    int  old = *val;

    *val += 1000;
    printf("tid=%lu, old=%d, val=%d\n", pthread_self(), old, *val);
    if(*val % 2) sleep(1);
}

int main(int argc, char **argv)
{
    tpool_t *tm;
    int     *vals;
    size_t   i;

    tm   = tpool_create(num_threads);
    vals = calloc(num_items, sizeof(*vals));
    if(!vals) return 1;

    for (i=0; i<num_items; i++) {
        vals[i] = i;
        tpool_add_work(tm, worker, vals+i);
    }
    
    tpool_destroy(tm);
    // tpool_wait(tm);


    // for (i=0; i<num_items; i++) {
    //     printf("%d\n", vals[i]);
    // }

    free(vals);

    return 0;
}