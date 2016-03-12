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

typedef double vertex_data_type;
typedef double edge_data_type;
typedef distributed_graph<vertex_data_type, edge_data_type> graph_type;

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

bool edges_parser(graph_type& graph, const std::string& filename,
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

vertex_data_type pagerank_map(graph_type::edge_type edge,
                              graph_type::vertex_type other) {
  return other.data() / other.num_out_edges();
}

void pagerank(graph_type::vertex_type vertex) {
  vertex.data() = 0.15 + 0.85 *
    warp::map_reduce_neighborhood(vertex, IN_EDGES, pagerank_map);
}

struct pagerank_writer {
  std::string save_vertex(graph_type::vertex_type v) {
    std::stringstream strm;
    strm << v.id() << "\t" << v.data() << "\n";
    return strm.str();
  }

  std::string save_edge(graph_type::edge_type e) { return ""; }
};

int main(int argc, char** argv) {
  timeval start_pulling;
  gettimeofday(&start_pulling, NULL);
  mpi_tools::init(argc, argv);
  distributed_control dc;
  command_line_options clopts("PageRank");
  string vertices_dir;
  string edges_dir;
  clopts.attach_option("vertices_dir", vertices_dir, "vertices_dir");
  clopts.attach_option("edges_dir", edges_dir, "edges_dir");
  size_t iterations = 5;
  clopts.attach_option("iterations", iterations,
                       "Number of asynchronous iterations to run");
  string saveprefix;
  clopts.attach_option("saveprefix", saveprefix,
                       "Prefix to save the output pagerank in");
  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }
  graph_type graph(dc, clopts);
  graph.load(vertices_dir, vertices_parser);
  graph.load(edges_dir, edges_parser);
  graph.finalize();
  timeval end_pulling;
  gettimeofday(&end_pulling, NULL);
  for (size_t i = 0; i < iterations; ++i) {
    warp::parfor_all_vertices(graph, pagerank);
    std::cout << "Iteration " << i << " complete\n";
  }
  timeval start_pushing;
  gettimeofday(&start_pushing, NULL);
  if (saveprefix != "") {
    graph.save(saveprefix, pagerank_writer(),
               false,    // do not gzip
               true,     // save vertices
               false);   // do not save edges
  }
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
