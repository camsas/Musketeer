#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/time.h>

#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"

using namespace graphchi;
using namespace std;

typedef double VertexDataType;
typedef double EdgeDataType;

struct PageRank : public GraphChiProgram<VertexDataType, EdgeDataType> {

  void before_iteration(int iteration, graphchi_context &ginfo) {
  }

  void after_iteration(int iteration, graphchi_context &ginfo) {
  }

  void before_exec_interval(vid_t window_st, vid_t window_en,
                            graphchi_context &ginfo) {
  }

  void update(graphchi_vertex<VertexDataType, EdgeDataType> &v,
              graphchi_context &ginfo) {
    if (ginfo.iteration == 0) {
      EdgeDataType ver_val = v.get_data() / v.num_outedges();
      for (int i = 0; i < v.num_outedges(); i++) {
        v.outedge(i)->set_data(ver_val);
      }
    } else {
      VertexDataType vertex_val = 0;
      for (int i = 0; i < v.num_inedges(); i++) {
        EdgeDataType in_edge_val = v.inedge(i)->get_data();
        vertex_val += in_edge_val;
      }
      vertex_val = 0.15 + 0.85 * vertex_val;
      v.set_data(vertex_val);
      if (v.num_outedges() > 0) {
        vertex_val /= v.num_outedges();
        for (int i = 0; i < v.num_outedges(); i++) {
          v.outedge(i)->set_data(vertex_val);
        }
      }
    }
  }

};

int main(int argc, const char** argv) {
  graphchi_init(argc, argv);
  metrics met("PageRank");
  int niters = get_option_int("niters", 5);
  string edges_filename = get_option_string("file");
  bool scheduler = false;
  timeval start_proc;
  gettimeofday(&start_proc, NULL);
  // Process input file if not already processed.
  int num_shards = convert_if_notexists<EdgeDataType>(
      edges_filename, get_option_string("nshards", "auto"));
  graphchi_engine<EdgeDataType, EdgeDataType> engine(
      edges_filename, num_shards, scheduler, met);
  timeval end_proc;
  gettimeofday(&end_proc, NULL);
  engine.set_modifies_inedges(false);
  PageRank program;
  engine.run(program, niters);
  timeval end_run;
  gettimeofday(&end_run, NULL);
  long loading_data = end_proc.tv_sec - start_proc.tv_sec;
  long run_time = end_run.tv_sec - end_proc.tv_sec;
  cout << "LOADING DATA: " << loading_data << endl;
  cout << "RUN TIME: " << run_time << endl;
  metrics_report(met);
  return 0;
}
