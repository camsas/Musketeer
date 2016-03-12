/* Copyright (c) 2007, Stanford University
 * Copyright (c) 2013, Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Stanford University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <strings.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sched.h>
#include "application.hh"
#include "defsplitter.hh"
#include "bench.hh"

// Added for HDFS support
#include "jni.h"
#include "hdfs.h"

#define DEFAULT_NDISP 10

enum { with_value_modifier = 0 };

struct sort : public map_reduce {
    sort(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, "\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *)s1, (const char *)s2);
    }
    void map_function(split_t *ma) {
        char k[1024];
        size_t klen;
        split_word sw(ma);
        int i = 0;
        char* val = safe_malloc<char>(ma->length + 1);
        if (((char*)ma->data)[0] == '\n') {
          memcpy(val, ((char*)ma->data)+1, ma->length-1);
          val[ma->length-1] = 0;
        } else {
          memcpy(val, ma->data, ma->length);
          val[ma->length] = 0;
        }
        while (sw.fill(k, sizeof(k), klen)) {
            if (i == sort_column_)
              map_emit(k, val, klen);
            ++i;
        }
    }
    void reduce_function(void *key_in, void **vals_in, size_t vals_len) {
        long *vals = (long *) vals_in;
        for (uint32_t i = 0; i < vals_len; i++)
            reduce_emit(key_in, (void*)vals[i]);
    }
    int combine_function(void *key_in, void **vals_in, size_t vals_len) {
        assert(vals_in);
        return vals_len;
    }
    void *key_copy(void *src, size_t s) {
        char *key = safe_malloc<char>(s + 1);
        memcpy(key, src, s);
        key[s] = 0;
        return key;
    }
    int final_output_compare(const keyval_t *kv1, const keyval_t *kv2) {
        if (alphanumeric_) {
            if (direction_)
                return -strcmp((char *)kv1->key, (char *)kv2->key);
            else
                return strcmp((char *)kv1->key, (char *)kv2->key);
        } else {
          int i1 = atoi((char*)kv1->key);
          int i2 = atoi((char*)kv2->key);
          return direction_ ? (i2 - i1) : -(i2 - i1);
        }
    }
    bool has_value_modifier() const {
        return with_value_modifier;
    }
    void set_sort_column(int col) {
        assert(col >= 0);
        sort_column_ = col;
    }
    void set_direction(int dir) {
        direction_ = (dir != 0);
    }
    void set_alphanumeric(int an) {
        alphanumeric_ = (an != 0);
    }
  private:
    defsplitter s_;
    int sort_column_;
    int direction_;
    int alphanumeric_;
};

static void print_top(xarray<keyval_t> *wc_vals, size_t ndisp) {
    size_t occurs = 0;
    printf("\nsort: results (TOP %zd from %zu keys):\n",
           ndisp, wc_vals->size());
    ndisp = std::min(ndisp, wc_vals->size());
    for (size_t i = 0; i < ndisp; i++) {
      keyval_t *w = wc_vals->at(i);
      printf("%s\n", (char *)w->val);
    }
}

static void output_all(xarray<keyval_t> *wc_vals, hdfsFS fs, hdfsFile fout) {
    char buf[100];
    for (uint32_t i = 0; i < wc_vals->size(); i++) {
      keyval_t *w = wc_vals->at(i);
      sprintf(buf, "%s\n", (char *)w->val);
      hdfsWrite(fs, fout, buf, strlen(buf));
    }
}

static void usage(char *prog) {
    printf("usage: %s [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -a : alphanumeric sort\n");
    printf("  -o filename : save output to a file\n");
    printf("  -c sort_col : sort by column in input\n");
    printf("  -d direction : zero for ascending (default), non-zero for descending\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 5, reduce_tasks = 0;
    int quiet = 0;
    int c;
    int sort_column = 0;
    int direction = 0;  // ASC by default
    int alphanumeric = 0;
    if (argc < 1)
      usage(argv[0]);
    char* fn = "/tmp/{{CLASS_NAME}}_{{REL_NAME}}_in";
    char* outfn = "{{OUTPUT_PATH}}/part-r-00000";
    hdfsFile fout = NULL;

    /* suck input file from HDFS */
    hdfsFS fs = hdfsConnect("freestyle.private.srg.cl.cam.ac.uk", 8020);
    hdfsFS localfs = hdfsConnect(NULL, 0);
    int res = hdfsCopy(fs, "{{INPUT_PATH}}", localfs, fn);
    printf("hdfsCopy: %d\n", res);

    while ((c = getopt(argc, argv, "p:s:l:m:r:qao:c:d:")) != -1) {
      switch (c) {
      case 'p':
        nprocs = atoi(optarg);
        break;
      case 'l':
        ndisp = atoi(optarg);
        break;
      case 'm':
        map_tasks = atoi(optarg);
        break;
      case 'r':
        reduce_tasks = atoi(optarg);
        break;
      case 'q':
        quiet = 1;
        break;
      case 'a':
        alphanumeric = 1;
        break;
      case 'o':
        outfn = optarg;
        break;
      case 'c':
        sort_column = atoi(optarg);
        break;
      case 'd':
        direction = atoi(optarg);
        break;
      default:
        usage(argv[0]);
        exit(EXIT_FAILURE);
        break;
      }
    }
    /* prepare output file */
    fout = hdfsOpenFile(fs, outfn, O_WRONLY | O_CREAT, 0, 0, 0);
    if (!fout) {
      fprintf(stderr, "unable to open %s: %s\n", optarg,
              strerror(errno));
      exit(EXIT_FAILURE);
    }
    mapreduce_appbase::initialize();
    /* get input file */
    std::string fnp = fn;
    fnp += "/part-r-00000";
    sort app(fnp.c_str(), map_tasks);
    app.set_sort_column(sort_column);
    app.set_direction(direction);
    app.set_alphanumeric(alphanumeric);
    app.set_ncore(nprocs);
    app.set_reduce_task(reduce_tasks);
    app.sched_run();
    app.print_stats();
    /* get the number of results to display */
    if (!quiet)
      print_top(&app.results_, ndisp);
    if (fout) {
      output_all(&app.results_, fs, fout);
      hdfsCloseFile(fs, fout);
    }
    app.free_results();
    mapreduce_appbase::deinitialize();
    /* Clean up HDFS state */
    hdfsDisconnect(fs);
    hdfsDisconnect(localfs);
    return 0;
}
