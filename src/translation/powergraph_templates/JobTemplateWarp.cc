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

#include <vector>
#include <string>
#include <fstream>
#include <sys/time.h>

#include <graphlab.hpp>
#include <graphlab/engine/warp_graph_mapreduce.hpp>
#include <graphlab/engine/warp_parfor_all_vertices.hpp>
#include <graphlab/engine/warp_graph_transform.hpp>

using namespace graphlab;
using namespace std;

typedef {{VERTEX_DATA_TYPE}} vertex_data_type;
typedef {{EDGE_DATA_TYPE}} edge_data_type;
typedef distributed_graph<vertex_data_type, edge_data_type> graph_type;

bool line_edge_cost_parser(graph_type& graph, const std::string& filename,
                           const std::string& textline) {
  vertex_id_type v_src;
  vertex_id_type v_dst;
  edge_data_type e_cost;
  std::stringstream line(textline);
  line >> v_src >> v_dst >> e_cost;
  if (!line.fail() && v_src != v_dst) {
    graph.add_edge(v_src, v_dst, e_cost);
  }
  return true;
}

bool line_parser(graph_type& graph, const std::string& filename,
                 const std::string& textline) {
  vertex_id_type v_src;
  vertex_id_type v_dst;
  std::stringstream line(textline);
  line >> v_src >> v_dst;
  if (!line.fail() && v_src != v_dst) {
    graph.add_edge(v_src, v_dst);
  }
  return true;
}

bool vertices_parser(graph_type& graph, const std::string& filename,
                     const std::string& textline) {
  vertex_id_type v_id;
  vertex_data_type v_val;
  std::stringstream line(textline);
  line >> v_id >> v_val;
  if (!line.fail()) {
    graph.add_vertex(v_id, v_val);
  }
  return true;
}

void combiner(vertex_data_type& a, const vertex_data_type& b) {
  {{COMBINER}}
}

vertex_data_type map_{{CLASS_NAME}}(graph_type::edge_type edge,
                                    graph_type::vertex_type vertex) {
{{MAP_CODE}}
return ver_val;
}

void map_vertices(graph_type::vertex_type vertex) {
  vertex_data_type ver_val =
       warp::map_reduce_neighborhood(vertex, {{EDGES}}, map_{{CLASS_NAME}},
                                     combiner);
  {{MAP_VERTICES}}
  vertex.data() = ver_val;
}

struct {{CLASS_NAME}}_writer {

  string save_vertex(graph_type::vertex_type v) {
    stringstream strm;
    strm << v.id() << " " << v.data() << "\n";
    return strm.str();
  }

  string save_edge(graph_type::edge_type e) {
    return "";
  }

};

int main(int argc, char** argv) {
  timeval start_pulling;
  gettimeofday(&start_pulling, NULL);
  mpi_tools::init(argc, argv);
  distributed_control dc;
  global_logger().set_log_level(LOG_INFO);
  command_line_options clopts("{{CLASS_NAME}}");
  string graph_dir = "{{HDFS_MASTER}}{{EDGES_PATH}}";
  string format = "tsv";
  string output_dir = "{{HDFS_MASTER}}{{VERTICES_PATH}}";
  size_t iterations = {{N_ITERS}};
  clopts.attach_option("format", format, "The graph file format");
  clopts.attach_option("iterations", iterations,
                       "Number of asynchronous iterations to run");
  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << endl;
    return EXIT_FAILURE;
  }
  graph_type graph(dc, clopts);
  graph.load(output_dir, vertices_parser);
  if ({{HAS_EDGE_COST}}) {
    graph.load(graph_dir, line_edge_cost_parser);
  } else {
    graph.load(graph_dir, line_parser);
  }
  graph.finalize();
  timeval end_pulling;
  gettimeofday(&end_pulling, NULL);
  dc.cout() << "#vertices: " << graph.num_vertices() << endl
            << "#edges: " << graph.num_edges() << endl;
  for (size_t num_it = 0; num_it < iterations; ++num_it) {
    warp::parfor_all_vertices(graph, map_vertices);
    std::cout << "Iteration " << num_it << " complete\n";
  }
  timeval start_pushing;
  gettimeofday(&start_pushing, NULL);
  graph.save(output_dir, {{CLASS_NAME}}_writer(),
             false,    // do not gzip
             true,     // save vertices
             false);   // do not save edges
  timeval end_pushing;
  gettimeofday(&end_pushing, NULL);
  long load_time = end_pulling.tv_sec - start_pulling.tv_sec;
  long run_time = start_pushing.tv_sec - end_pulling.tv_sec;
  long time_pushing = end_pushing.tv_sec - start_pushing.tv_sec;
  char hostname[256];
  gethostname(hostname, sizeof(hostname));
  std::cout << "LOADING DATA ON " << hostname << ": " << load_time << std::endl;
  std::cout << "RUN TIME ON " << hostname << ": " << run_time << std::endl;
  std::cout << "PUSHING DATA ON " << hostname << ": " << time_pushing <<
    std::endl;
  mpi_tools::finalize();
  return EXIT_SUCCESS;
}
