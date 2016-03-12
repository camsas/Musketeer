#include <vector>
#include <string>
#include <fstream>
#include <sys/time.h>

#include <graphlab.hpp>

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


class pagerank :
  public ivertex_program<graph_type, vertex_data_type>, public IS_POD_TYPE {

public:

  vertex_data_type gather(icontext_type& context,
                          const graph_type::vertex_type& vertex,
                          graph_type::edge_type& edge) const {
    return (0.85 / edge.source().num_out_edges()) * edge.source().data();
  }

  void apply(icontext_type& context, graph_type::vertex_type& vertex,
             const gather_type& total) {
    const double newval = total + 0.15;
    vertex.data() = newval;
  }

  edge_dir_type scatter_edges(icontext_type& context,
                              const graph_type::vertex_type& vertex) const {
    return OUT_EDGES;
  }

  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    context.signal(edge.target());
  }

};

struct pagerank_writer {
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
  graphlab::mpi_tools::init(argc, argv);
  distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

  command_line_options clopts("PageRank");
  string graph_dir;
  string exec_type = "synchronous";
  size_t iterations = 5;
  string vertices_dir;
  string edges_dir;
  clopts.attach_option("vertices_dir", vertices_dir, "vertices_dir");
  clopts.attach_option("edges_dir", edges_dir, "edges_dir");
  clopts.attach_option("iterations", iterations, "Num max iter");
  clopts.get_engine_args().set_option("max_iterations", iterations);
  string saveprefix;
  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the resultant pagerank to a "
                       "sequence of files with prefix saveprefix");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << endl;
    return EXIT_FAILURE;
  }

  graph_type graph(dc, clopts);
  graph.load(vertices_dir, vertices_parser);
  graph.load(edges_dir, edges_parser);
  graph.finalize();
  timeval end_pulling;
  gettimeofday(&end_pulling, NULL);
  dc.cout() << "#vertices: " << graph.num_vertices()
            << "#edges: " << graph.num_edges() << endl;
  omni_engine<pagerank> engine(dc, graph, exec_type, clopts);
  engine.signal_all();
  engine.start();
  const float runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime
            << " seconds." << endl;
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
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
}
