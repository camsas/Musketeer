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

#include <graphlab.hpp>

using namespace std;

typedef {{VERTEX_DATA_TYPE}} vertex_data_type;
typedef {{EDGE_DATA_TYPE}} edge_data_type;
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

void init_vertex(graph_type::vertex_type& vertex) {
  vertex.data() = 1;
}

class {{CLASS_NAME}} :
  public graphlab::ivertex_program<graph_type, {{VERTEX_DATA_TYPE}}>,
  public graphlab::IS_POD_TYPE {

public:

  edge_dir_type gather_edges(icontext_type& context,
                             const graph_type::vertex_type& vertex) const {
    return graphlab::{{GATHER_EDGES}};
  }

  {{VERTEX_DATA_TYPE}} gather(icontext_type& context,
                              const graph_type::vertex_type& vertex,
                              graph_type::edge_type& edge) const {
    {{GATHER}}
    return 0.85 / edge.source().num_out_edges() * edge.source().data();
  }

  void apply(icontext_type& context, graph_type::vertex_type& vertex,
             const gather_type& total) {
    {{APPLY}}
    vertex.data() = total + 0.15;
  }

  edge_dir_type scatter_edges(icontext_type& context,
                              const graph_type::vertex_type& vertex) const {
    return graphlab::{{SCATTER_EDGES}}
  }

  void scatter(icontext_type& context, const graph_type::vertex_type& vertex,
               graph_type::edge_type& edge) const {
    {{SCATTER}}
    context.signal(edge.target());
  }

};

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
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);
  graphlab::command_line_options clopts("{{CLASS_NAME}}");
  string graph_dir = "{{HDFS_MASTER}}{{EDGES_PATH}}";
  string format = "tsv";
  string exec_type = "synchronous";
  string output_dir = "{{HDFS_MASTER}}{{VERTICES_PATH}}";
  string max_iterations = "{{N_ITERS}}";
  clopts.attach_option("max_iterations", max_iterations,
                       "Num of max iterations");
  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << endl;
    return EXIT_FAILURE;
  }
  graph_type graph(dc, clopts);
  graph.load_format(graph_dir, format);
  graph.finalize();
  dc.cout() << "#vertices: " << graph.num_vertices()
            << "#edges: " << graph.num_edges() << endl;
  graph.transform_vertices(init_vertex);
  graphlab::omni_engine<{{CLASS_NAME}}> engine(dc, graph, exec_type, clopts);
  engine.signal_all();
  engine.start();
  const float runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime << " seconds." << endl;

  graph.save(output_dir, {{CLASS_NAME}}_writer(),
             false,    // do not gzip
             true,     // save vertices
             false);   // do not save edges

  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
}
