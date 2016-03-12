/* Copyright (c) 2013, Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
 *
 * Portions originally Copyright (c) 2007, Stanford University
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

#include <map>
#include <vector>

//#define HEAP_PROFILE
//#define CPU_PROFILE

// Glog logging
#include <glog/logging.h>

// Google profiler
#ifdef CPU_PROFILE
#include <google/profiler.h>
#endif
#ifdef HEAP_PROFILE
#include <google/heap-profiler.h>
#endif

// Shared Musketeer utility functions
#include "utils.h"

#if USE_HDFS == 1
// HDFS access support
#include "hdfs_utils.h"
#endif

// default buffer size is 4K
#define TEMP_BUF_SIZE (1 << 12)

enum { with_value_modifier = 0 };

struct {{CLASS_NAME}} : public map_reduce {
    {{CLASS_NAME}}(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, "\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *)s1, (const char *)s2);
    }
    void map_function(split_t *ma) {
{{INPUT_CODE}}
{{MAP_CODE}}
    }
    void reduce_function(void *key_in, void **vals_in, size_t vals_len) {
{{REDUCE_CODE}}
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
/*    int final_output_compare(const keyval_t *kv1, const keyval_t *kv2) {
        if (alphanumeric_) {
            if (direction_)
                return -strcmp((char *)kv1->key_, (char *)kv2->key_);
            else
                return strcmp((char *)kv1->key_, (char *)kv2->key_);
        } else {
          int i1 = atoi((char*)kv1->key_);
          int i2 = atoi((char*)kv2->key_);
          return direction_ ? (i2 - i1) : -(i2 - i1);
        }
    }
    bool has_value_modifier() const {
        return with_value_modifier;
    }*/
    void set_direction(int dir) {
        direction_ = (dir != 0);
    }
    void set_alphanumeric(int an) {
        alphanumeric_ = (an != 0);
    }
  private:
    defsplitter s_;
    int direction_;
    int alphanumeric_;
    // Additional variables
    {{MAP_VARIABLES_CODE}}
};

int main(int argc, char *argv[]) {
    // Set up glog for logging output
    google::InitGoogleLogging(argv[0]);

    if (argc < 1)
	usage(argv[0]);

    const char* in_filename = "/tmp/{{CLASS_NAME}}_{{REL_NAME}}_in";

    /* process command line options */
    options_t opts;
    opts.num_procs = 0;
    opts.num_map_tasks = 0;
    opts.num_reduce_tasks = 0;
    opts.quiet = 1;
    opts.sort_direction = 0;  // default: ascending
    opts.sort_alpha = 0;
    opts.out_filename = "{{OUTPUT_PATH}}/part-r-00000";
    process_options(argc, argv, &opts);

    /* build collection of input paths */
    std::vector<std::string> input_paths;
    {{INPUT_PATH}}

#if USE_HDFS == 1
    /* suck input file from HDFS */
    timeval pull_start_time, pull_end_time;
    gettimeofday(&pull_start_time, NULL);
    if (input_paths.size() == 1) {
      if (!copyAndMergeFilesFromHDFSNoPrefix(input_paths, in_filename))
          printf("Failed to load inputs from HDFS!\n");
    } else {
      if (!copyAndMergeFilesFromHDFS(input_paths, in_filename))
          printf("Failed to load inputs from HDFS!\n");
    }
    gettimeofday(&pull_end_time, NULL);
    uint64_t pull_time = pull_end_time.tv_sec - pull_start_time.tv_sec;
    printf("PULLING DATA: %u\n", pull_time);
#endif

    timeval load_start_time, load_end_time;
    gettimeofday(&load_start_time, NULL);
    /* start things up */
#ifdef HEAP_PROFILE
    HeapProfilerStart("{{CLASS_NAME}}");
#endif
    mapreduce_appbase::initialize();
    /* get input file */
    std::string fnp = in_filename;
    {{CLASS_NAME}} app(fnp.c_str(), opts.num_map_tasks);
    app.set_direction(opts.sort_direction);
    app.set_alphanumeric(opts.sort_alpha);
    app.set_ncore(opts.num_procs);
    app.set_reduce_task(opts.num_reduce_tasks);
    /* done loading, emit stats */
    gettimeofday(&load_end_time, NULL);
    uint64_t load_time = load_end_time.tv_sec - load_start_time.tv_sec;
    printf("LOADING DATA: %u\n", load_time);
    /* go! */
    printf("starting MapReduce execution...\n");
    timeval run_start_time, run_end_time;
    gettimeofday(&run_start_time, NULL);
    /* actual MR run */
#ifdef CPU_PROFILE
    ProfilerStart("/tmp/cpuprofile");
#endif
    app.sched_run();
#ifdef CPU_PROFILE
    ProfilerStop();
#endif
#ifdef HEAP_PROFILE
    HeapProfilerDump("MR done");
    HeapProfilerStop();
#endif
    /* MR run done */
    gettimeofday(&run_end_time, NULL);
    uint64_t run_time = run_end_time.tv_sec - run_start_time.tv_sec;
    printf("RUN TIME: %u\n", run_time);
    app.print_stats();
    /* prepare output file */
    if (!opts.quiet)
	print_top(&app.results_, 10);
#if USE_HDFS == 1
    timeval push_start_time, push_end_time;
    gettimeofday(&push_start_time, NULL);
    if (!writeResultsToHDFS(opts.out_filename, &app.results_))
        printf("Failed to write results out the HDFS!");
    gettimeofday(&push_end_time, NULL);
    uint64_t push_time = push_end_time.tv_sec - push_start_time.tv_sec;
    printf("PUSHING DATA: %u\n", push_time);
#else
    FILE* localOutFD = fopen(opts.out_filename, "w");
    if (!localOutFD) {
	fprintf(stderr, "unable to open %s: %s\n", opts.out_filename,
		strerror(errno));
	exit(EXIT_FAILURE);
    } else {
        printf("Now writing output to local file %s...\n", opts.out_filename);
        output_all_local(&app.results_, localOutFD);
        fclose(localOutFD);
    }
#endif
    app.free_results();
    mapreduce_appbase::deinitialize();

    return 0;
}
