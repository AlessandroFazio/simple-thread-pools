#define _GNU_SOURCE
#include <sched.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include "tpool.h"

struct tpool_work
{
    thread_func_t func;
    void *arg;
    struct tpool_work *next;
};
typedef struct tpool_work tpool_work_t;

struct tpool 
{
    tpool_work_t *work_first;
    tpool_work_t *work_last;
    pthread_mutex_t work_mutex;
    pthread_cond_t work_cond;
    pthread_cond_t working_cond;
    size_t working_cnt;
    size_t thread_cnt;
    bool volatile stop;
};

static tpool_work_t *tpool_work_create(thread_func_t func, void *arg) 
{
    tpool_work_t *work;

    if(func == NULL) return NULL;

    work = malloc(sizeof(*work));
    if(!work) {
        perror("An error occurred while allocaing memory for tpool_work_t.\n");
        return NULL;
    }
    work->func = func;
    work->arg = arg;
    work->next = NULL;
    return work;
}

static void tpool_work_destroy(tpool_work_t *work) 
{
    if(work == NULL) {
        return;
    }
    free(work);
}

static tpool_work_t *tpool_work_get(tpool_t *tm) 
{
    tpool_work_t *work;

    if(tm == NULL) return NULL;

    work = tm->work_first;
    if(work == NULL) return NULL;

    if(work->next == NULL) {
        tm->work_first = NULL;
        tm->work_last = NULL;
    } else {
        tm->work_first = work->next;
    }

    return work;
}

static void *tpool_worker(void *arg) 
{
    tpool_t *tm = arg;
    tpool_work_t *work;

    while(1) {
        pthread_mutex_lock(&(tm->work_mutex));

        while(tm->work_first == NULL && !tm->stop) {
            pthread_cond_wait(&(tm->work_cond), &(tm->work_mutex));
        }

        work = tpool_work_get(tm);

        tm->working_cnt++;
        pthread_mutex_unlock(&(tm->work_mutex));

        if(work != NULL) {
            work->func(work->arg);
            tpool_work_destroy(work);
        }

        pthread_mutex_lock(&(tm->work_mutex));
        tm->working_cnt--;
        if(!tm->stop && tm->working_cnt == 0 && tm->work_first == NULL) {
            pthread_cond_signal(&(tm->working_cond));
        }

        if(tm->stop && tm->work_first == NULL) break;

        pthread_mutex_unlock(&(tm->work_mutex));
    }

    tm->thread_cnt--;
    pthread_cond_signal(&(tm->working_cond));
    pthread_mutex_unlock(&(tm->work_mutex));
    return NULL;
}

int nprocs()
{
  cpu_set_t cs;
  CPU_ZERO(&cs);
  sched_getaffinity(0, sizeof(cs), &cs);
  return CPU_COUNT(&cs);
}

tpool_t *tpool_create(size_t num) 
{
    tpool_t *tm;
    pthread_t thread;
    size_t i;

    if(num == 0) num = nprocs();

    tm = calloc(1, sizeof(*tm));
    if(!tm) return NULL;
    
    tm->thread_cnt = num;

    pthread_mutex_init(&(tm->work_mutex), NULL);
    pthread_cond_init(&(tm->work_cond), NULL);
    pthread_cond_init(&(tm->working_cond), NULL);

    tm->work_first = NULL;
    tm->work_last = NULL;

    for(i=0; i<num; i++) {
        pthread_create(&thread, NULL, tpool_worker, tm);
        pthread_detach(thread);
    }

    return tm;
}

void tpool_destroy(tpool_t *tm) 
{
    tpool_work_t *work;
    tpool_work_t *work2;

    if(tm == NULL) return;

    tm->stop = true;

    pthread_cond_broadcast(&(tm->work_cond));
    tpool_wait(tm);

    pthread_mutex_destroy(&(tm->work_mutex));
    pthread_cond_destroy(&(tm->work_cond));
    pthread_cond_destroy(&(tm->working_cond));

    free(tm);
}

bool tpool_add_work(tpool_t *tm, thread_func_t func, void *arg)
{
    tpool_work_t *work;

    if(tm == NULL) return false;
    if(tm->stop) return false;

    work = tpool_work_create(func, arg);
    if(work == NULL) return false;

    pthread_mutex_lock(&(tm->work_mutex));
    if(tm->work_first == NULL) {
        tm->work_first = work;
        tm->work_last = tm->work_first;
    } else {
        tm->work_last->next = work;
        tm->work_last = work;
    }

    pthread_cond_broadcast(&(tm->work_cond));
    pthread_mutex_unlock(&(tm->work_mutex));

    return true;
}

void tpool_wait(tpool_t *tm) 
{
    if(tm == NULL) return;

    pthread_mutex_lock(&(tm->work_mutex));
    while(1) {
        if((!tm->stop && tm->working_cnt != 0) || (tm->stop && tm->thread_cnt != 0)) {
            pthread_cond_wait(&(tm->working_cond), &(tm->work_mutex));
        } 
        else break;
    }
    pthread_mutex_unlock(&(tm->work_mutex));
}