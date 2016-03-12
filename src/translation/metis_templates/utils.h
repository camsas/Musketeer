// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#ifndef METIS_GENERATED_UTILS_H
#define METIS_GENERATED_UTILS_H

#include <stdint.h>

#include <string>
#include <sstream>
#include <vector>

#define input_id_num_cols(I) __input_ ## I ## _num_cols;
#define input_name_num_cols(N) __ ## N ## _num_cols;

typedef struct {
  uint32_t num_procs;
  uint32_t num_map_tasks;
  uint32_t num_reduce_tasks;
  bool quiet;
  bool sort_alpha;
  bool sort_direction;
  char* out_filename;
} options_t;

static void usage(char *prog) {
    printf("usage: %s [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -a : alphanumeric sort\n");
    printf("  -o filename : save output to a file\n");
    printf("  -d sort_direction : zero for ascending (default), non-zero for descending\n");
    exit(EXIT_FAILURE);
}


static void process_options(int argc, char* argv[], options_t* opts) {
  int c;
  while ((c = getopt(argc, argv, "p:s:m:r:qao:c:d:")) != -1) {
    switch (c) {
    case 'p':
      opts->num_procs = atoi(optarg);
      break;
    case 'm':
      opts->num_map_tasks = atoi(optarg);
      break;
    case 'r':
      opts->num_reduce_tasks = atoi(optarg);
      break;
    case 'q':
      opts->quiet = 1;
      break;
    case 'a':
      opts->sort_alpha = 1;
      break;
    case 'o':
      opts->out_filename = optarg;
      break;
    case 'd':
      opts->sort_direction = atoi(optarg);
      break;
    default:
      usage(argv[0]);
      exit(EXIT_FAILURE);
      break;
    }
  }
}

struct split_numbers {
  split_numbers(split_t *ma) : ma_(ma), pos_(0) {
    assert(ma_ && ma_->data);
  }
  char *fill(char *k, size_t maxlen, size_t &klen) {
    char *d = (char *)ma_->data;
    klen = 0;
    for (; pos_ < ma_->length && !number(d[pos_]); ++pos_)
      ;
    if (pos_ == ma_->length)
      return NULL;
    char *index = &d[pos_];
    for (; pos_ < ma_->length && number(d[pos_]); ++pos_) {
      k[klen++] = d[pos_];
      assert(klen < maxlen);
    }
    k[klen] = 0;
    return index;
  }
 private:
  bool number(char c) {
    return c >= '0' && c <= '9';
  }
  split_t *ma_;
  size_t pos_;
};

struct split_lines {
  split_lines(split_t *ma) : ma_(ma), pos_(0) {
    assert(ma_ && ma_->data);
  }
  char *fill(char *k, size_t maxlen, size_t &klen) {
    char *d = (char *)ma_->data;
    klen = 0;
    for (; pos_ < ma_->length && !nolinebreak(d[pos_]); ++pos_);
    if (pos_ == ma_->length) {
      return NULL;
    }
    char *index = &d[pos_];
    for (; pos_ < ma_->length && nolinebreak(d[pos_]); ++pos_) {
      k[klen++] = d[pos_];
      assert(klen < maxlen);
    }
    k[klen] = 0;
    return index;
  }
 private:
  bool nolinebreak(char c) {
    return c != '\n';
  }
  split_t *ma_;
  size_t pos_;
};

std::vector<std::string>& str_split(const std::string &s, char delim,
                                    std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> str_split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  str_split(s, delim, elems);
  return elems;
}

std::vector<const char*> cstr_split(char* s, char delim) {
  std::vector<const char*> elems;
  size_t len = strlen(s);
  for (uint64_t i = 0; i < len; ++i) {
    if (s[i] == ' ') {
      s[i] = '\0';
      assert(i + 1 < len);
      elems.push_back(&s[i+1]);
    }
  }
  return elems;
}


static void print_top(xarray<keyval_t> *wc_vals, size_t ndisp) {
  printf("\nresults (TOP %zd from %zu keys):\n",
         ndisp, wc_vals->size());
  ndisp = std::min(ndisp, wc_vals->size());
  for (size_t i = 0; i < ndisp; i++) {
    keyval_t *w = wc_vals->at(i);
    printf("%s\n", (char *)w->val);
  }
}

static void output_all_local(xarray<keyval_t> *wc_vals, FILE* fout) {
  printf("writing out %jd rows with values\n", wc_vals->size());
  char output_buffer[1 << 17];  // 32 KB
  uint64_t filled_up_to = 0;
  for (uint32_t i = 0; i < wc_vals->size(); i++) {
    keyval_t *w = wc_vals->at(i);
    size_t len = strlen((char*)w->val);
    // If this line won't fit into the buffer, spill it
    if (filled_up_to + len + 1 >= (1 << 17)) {
      fwrite(&output_buffer[0], filled_up_to, 1, fout);
      filled_up_to = 0;
    }
    strncpy(&output_buffer[filled_up_to], (char*)w->val, len);
    filled_up_to += len;
    output_buffer[filled_up_to++] = '\n';
  }
  // Final spill
  fwrite(&output_buffer[0], filled_up_to, 1, fout);
}

#endif  // METIS_GENERATED_UTILS_H
