#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>

#include <vsl_dp_experimental/kv.h>

const char dev[] = "/dev/fct0";

int fd = 0;
int kvid = -1;
int bucket = 0;

#define NUMKEYS 1024
#define MAXBLOCK 1024
//((1024*1024) - 1025)
#define KEYLENGTH 16
#define MAX_BUCKETS 1024
#define MAX_BATCH_SIZE KV_MAX_VECTORS

char *keys[NUMKEYS];
size_t sizes[NUMKEYS];
void *block;

static void setup_kvstore(void) {
    if ((fd = open(dev, O_RDWR)) == -1) {
       fprintf(stderr, "Failed to open device %s: %s\n", dev, strerror(errno));
       exit(EXIT_FAILURE);
    }
    fprintf(stdout, "%d = open(\"%s\", O_RDWR)\n", fd, dev);

    kvid = kv_create(fd, 0, MAX_BUCKETS, true);
    if (kvid == -1) {
        fprintf(stderr, "Failed to create kvstore: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "%d = kv_create(%d, 0, %d, true)\n", kvid, fd, MAX_BUCKETS);
    // Now destroy it and recreate it to ensure that all of the pools
    // are gone..
    int ret = kv_destroy(kvid);
    fprintf(stdout, "%d = kv_destroy(%d)\n", ret, kvid);
    kvid = kv_create(fd, 0, MAX_BUCKETS, true);
    if (kvid == -1) {
        fprintf(stderr, "Failed to create kvstore: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "%d = kv_create(%d, 0, %d, true)\n", kvid, fd, MAX_BUCKETS);
    bucket = kv_pool_create(kvid);
    if (bucket == -1) {
        fprintf(stderr, "Failed to create pool: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    fprintf(stdout, "%d = kv_pool_create(%d)\n", bucket, kvid);
}

static void setup_testdata(void) {
   srandom(0);
   posix_memalign(&block, 512, MAXBLOCK);
   for (int ii = 0; ii < NUMKEYS; ++ii) {
      char buffer[40];
      sprintf(buffer, "%04d%04d%04d%04d", ii, ii, ii, ii);
      keys[ii] = strdup(buffer);
      sizes[ii] = (random() % MAXBLOCK) + 1;
   }
}

static void test_sequential(void) {
   for (int jj = 0; jj < 1024; ++jj) {
      for (int ii = 0; ii < 1024; ++ii) {
          if (kv_put(kvid, bucket, (kv_key_t*)keys[ii], KEYLENGTH,
                     block, sizes[ii], 0, true, 0) == -1) {
              fprintf(stderr, "kv_put(%d, %d, \"%s\", %u, %p, %d, 0, true, 0): failed %d\n",
                      kvid, bucket, keys[ii], KEYLENGTH, block,
                      (unsigned int)sizes[ii], errno);
              exit(EXIT_FAILURE);
          }
      }
   }
}

static void test_bulk(void) {
    kv_iovec_t keybuf[KV_MAX_VECTORS];
    fprintf(stderr, "MAX: %d\n", KV_MAX_VECTORS);

    for (int jj = 0; jj < 1024; ++jj) {
        int yy = 0;
        for (int ii = 0; ii < (1024/MAX_BATCH_SIZE); ++ii) {
            for (int xx = 0; xx < MAX_BATCH_SIZE; ++xx, ++yy) {
                keybuf[xx].key = (kv_key_t*)keys[yy];
                keybuf[xx].key_len = KEYLENGTH;
                keybuf[xx].value = block;
                keybuf[xx].value_len = sizes[yy];
                keybuf[xx].expiry = 0; // @todo fixme
                keybuf[xx].gen_count = 0; // @todo fixme
                keybuf[xx].replace = true;
                keybuf[xx].reserved1 = 0; // @todo fixme
            }
            if (kv_batch_put(kvid, bucket, keybuf, MAX_BATCH_SIZE) == -1) {
                fprintf(stderr, "batch put failed: %d\n", errno);
                for (int xx = 0; xx < MAX_BATCH_SIZE; ++xx) {
                    fprintf(stderr, "  {\n");
                    fprintf(stderr, "     key    : \"%s\"\n", keybuf[xx].key);
                    fprintf(stderr, "     key_len: %d\n", keybuf[xx].key_len);
                    fprintf(stderr, "     value  : %p\n", keybuf[xx].value);
                    fprintf(stderr, "     val_len: %u\n", keybuf[xx].value_len);
                    fprintf(stderr, "  }\n");
                }
                exit(EXIT_FAILURE);
            }
        }
    }
}

static void convert(const struct timespec *ts, struct timeval *tv) {
    tv->tv_sec = ts->tv_sec;
    tv->tv_usec = ts->tv_nsec / 1000;
}

static void timeit(const char *prefix, void (*func)(void)) {
    struct timespec start, stop;
    fprintf(stdout, "Running %s:.. ", prefix);
    fflush(stdout);
    clock_gettime(CLOCK_MONOTONIC, &start);
    func();
    clock_gettime(CLOCK_MONOTONIC, &stop);

    struct timeval t1, t2, result;
    convert(&start, &t1);
    convert(&stop, &t2);

    timersub(&t2, &t1, &result);
    fprintf(stdout, "Done (%u s, %lu usec)\n", (unsigned int)result.tv_sec,
            result.tv_usec);
}

int main(void) {
    setup_kvstore();
    setup_testdata();

    timeit("populating", test_sequential);

    for (int ii = 0; ii < 4; ++ii) {
        timeit("single", test_sequential);
        timeit("bulk", test_bulk);
    }

    // Destroy the kvstorage
    kv_destroy(kvid);
    close(fd);
    exit(EXIT_SUCCESS);
}
