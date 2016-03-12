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

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <stdio.h>

#include <iostream>
#include <vector>

#include "base/common.h"
#include "base/utils.h"
#include "core/daemon.h"
#include "core/daemon_connection.h"
#include "frontends/beeraph.h"
#include "frontends/operator_node.h"
#include "frontends/tree_traversal.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"
#include "scheduling/operator_scheduler.h"
#include "scheduling/scheduler_dynamic.h"

using namespace musketeer; // NOLINT
using namespace musketeer::core; // NOLINT
using namespace musketeer::framework; // NOLINT
using namespace musketeer::scheduling; // NOLINT

DEFINE_string(root_dir, "/home/icg27/", "Root Directory Path. By default "
              "Musketeer expects the frameworks to be install in root_dir."
              " If a framework is installed in a different directory then "
              "change its appropriate flag");
DEFINE_string(beer_query, "", "BEER DSL input file");
DEFINE_string(beeraph_templates_dir, "src/", "Beeraph_dir");
DEFINE_string(daemon_port, "20000",
              "Musketeer daemon port number used for submitting jobs");
DEFINE_string(force_framework, "",
              "Force a framework for all operators in the workflow");
DEFINE_string(generated_code_dir, "",
              "Directory into which to generate job code");
DEFINE_string(hdfs_input_dir, "", "HDFS directory where to store data");
DEFINE_bool(output_ir_dag_gv, false, "Print DAG in GraphViz format");
DEFINE_bool(optimise_ir_dag, true, "Activate DAG optimisations");
DEFINE_bool(run_daemon, true, "Run in daemon mode.");
DEFINE_string(tmp_data_dir, "/tmp/",
              "Tmp directory to store data fetched from HDFS.");
DEFINE_string(use_frameworks, "hadoop-spark-graphchi-naiad-powergraph",
              "Frameworks that can be used. Dash separated");

// Scheduler flags.
DEFINE_bool(best_runtime, true, "Optimize for runtime or resource utilization");
DEFINE_bool(dry_run, false,
            "Do a dry run. Generate job code but do not dispatch the jobs");
DEFINE_string(dry_run_data_size_file, "",
              "The path to the file containing the workflow's data sizes");
DEFINE_uint64(max_scheduler_cost, 100000,
             "Maximum value the cost function can return");
DEFINE_bool(operator_merge, true, "Activates operator merge");
DEFINE_bool(populate_history, false,
            "True if the scheduler should read the expected data size of all"
            " the operators");
DEFINE_double(time_to_cost, 1, "Time to cost scalling factor");
DEFINE_bool(use_heuristic, true, "Use scheduler heuristic");
DEFINE_bool(use_dynamic_scheduler, true, "Use dynamic scheduler");

// HDFS flags.
DEFINE_string(hdfs_master, "localhost", "HDFS namenode hostname");
DEFINE_string(hdfs_port, "8020", "HDFS namenode port");

// GraphChi flags.
DEFINE_string(graphchi_dir, "", "GraphChi directiory");
DEFINE_string(graphchi_templates_dir, "src/translation/graphchi_templates/",
              "GraphChi templates directory");

// Hadoop flags.
DEFINE_string(hadoop_templates_dir, "src/translation/hadoop_templates/",
              "Hadoop templates directory");
DEFINE_string(hadoop_job_tracker_host, "localhost",
              "Hadoop job tracer hostname");
DEFINE_int32(hadoop_job_tracker_port, 50030, "Hadoop job tracker port number");

// Metis flags.
DEFINE_string(metis_templates_dir, "src/translation/metis_templates/",
              "Metis templates directory");
DEFINE_string(metis_dir, "", "Location of Metis");
DEFINE_bool(metis_use_hdfs, false,
            "Whether Metis jobs should read from HDFS or local files");
DEFINE_bool(metis_debug_binaries, false, "Run Metis binaries through gdb");
DEFINE_bool(metis_strace_binaries, false, "Run Metis binaries through strace");
DEFINE_bool(metis_heapprofile, false,
            "Run Metis binaries through heap profiler");
DEFINE_bool(metis_cpuprofile, false, "Run Metis binaries through CPU profiler");
DEFINE_string(metis_glog_v, "1", "GLOG logging level for Metis binaries");

// Naiad flags.
DEFINE_string(naiad_dir, "", "Naiad directory");
DEFINE_string(naiad_templates_dir, "src/translation/naiad_templates/",
              "Naiad templates directory");
DEFINE_string(naiad_hosts_file, "",
              "Location of the file containing the Naiad hostnames");
DEFINE_int32(naiad_num_workers, 1, "Naiad number of workers");
DEFINE_int32(naiad_num_threads, 1, "Naiad number of threads/worker");

// PowerGraph flags.
DEFINE_string(powergraph_dir, "", "PowerGraph directory");
DEFINE_string(powergraph_templates_dir, "src/translation/powergraph_templates/",
              "PowerGraph templates directory");
DEFINE_int32(powergraph_num_workers, 1, "Number of PowerGraph nodes");

// PowerLyra flags.
DEFINE_string(powerlyra_dir, "", "PowerLyra directory");
DEFINE_string(powerlyra_templates_dir, "src/translation/powergraph_templates/",
              "PowerLyra templates directory");
DEFINE_int32(powerlyra_num_workers, 1, "Number of PowerLyra nodes");

// Spark flags.
DEFINE_string(spark_dir, "", "Location of Spark");
DEFINE_string(spark_master, "spark://localhost:7077", "Spark Master Location");
DEFINE_string(spark_version, "0.9.0-incubating", "Spark Version Number");
DEFINE_string(scala_version, "2.10.3", "Scala Version");
DEFINE_string(scala_major_version, "2.10", "Scala Major Version");
DEFINE_string(spark_templates_dir, "src/translation/spark_templates/",
              "Spark templates directory");
DEFINE_string(spark_web_ui_host, "localhost", "Spark Web UI Host");
DEFINE_int32(spark_web_ui_port, 8080, "Spark Web UI Port");

// Wildcherry flags.
DEFINE_string(wildcherry_templates_dir, "src/translation/wildcherry_templates/",
              "WildCherry templates directory");
DEFINE_string(wildcherry_dir, "", "Location of Wildcherry");
DEFINE_bool(wildcherry_use_hdfs, false,
            "Whether Wildcherry jobs should read from HDFS or local files");

inline void init(int argc, char *argv[]) {
  // Set up usage message.
  string usage("Runs a job.  Sample usage:\nmusketeer --beer_query test.rap");
  google::SetUsageMessage(usage);

  // Use gflags to parse command line flags
  // The final (boolean) argument determines whether gflags-parsed flags should
  // be removed from the array (if true), otherwise they will re-ordered such
  // that all gflags-parsed flags are at the beginning.
  google::ParseCommandLineFlags(&argc, &argv, false);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);
}

bool set_up_daemon(int port, SchedulerInterface* scheduler) {
  LOG(INFO) << "Set Up Daemon on port " << port;
  // Create new thread
  boost::asio::io_service io_service;
  Daemon server(&io_service, port, scheduler);
  uint64_t num_exec = io_service.run();
  LOG(INFO) << "Daemon Set up on port " << port;
  return (num_exec > 0);
}

map<string, FrameworkInterface*> AddFrameworks(string frams) {
  map<string, FrameworkInterface*> frameworks =
    map<string, FrameworkInterface*>();
  vector<string> fmws;
  boost::split(fmws, frams, boost::algorithm::is_any_of("-"));
  for (vector<string>::iterator it = fmws.begin(); it != fmws.end(); ++it) {
    if (!it->compare("hadoop")) {
      frameworks["hadoop"] = new HadoopFramework();
      LOG(INFO) << "Adding Hadoop Framework";
    } else if (!it->compare("spark")) {
      frameworks["spark"] = new SparkFramework();
      LOG(INFO) << "Adding Spark Framework";
    } else if (!it->compare("metis")) {
      frameworks["metis"] = new MetisFramework();
      LOG(INFO) << "Adding Metis Framework";
    } else if (!it->compare("naiad")) {
      frameworks["naiad"] = new NaiadFramework();
      LOG(INFO) << "Adding Naiad Framework";
    } else if (!it->compare("graphchi")) {
      frameworks["graphchi"] = new GraphChiFramework();
      LOG(INFO) << "Adding GraphChi Framework";
    } else if (!it->compare("powergraph")) {
      frameworks["powergraph"] = new PowerGraphFramework();
      LOG(INFO) << "Adding PowerGraph Framework";
    } else if (!it->compare("powerlyra")) {
      frameworks["powerlyra"] = new PowerLyraFramework();
    } else if (!it->compare("wildcherry")) {
      frameworks["wildcherry"] = new WildCherryFramework();
      LOG(INFO) << "Adding WildCherry Framework";
    }
  }
  return frameworks;
}

int main(int argc, char *argv[]) {
  // TODO(malte): Possibly move this elsewhere if we don't expect to run
  // interactively from here.
  init(argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  if (FLAGS_generated_code_dir == "") {
    FLAGS_generated_code_dir = FLAGS_root_dir;
  }
  if (FLAGS_hdfs_input_dir == "") {
    FLAGS_hdfs_input_dir = FLAGS_root_dir;
  }
  if (FLAGS_graphchi_dir == "") {
    FLAGS_graphchi_dir = FLAGS_root_dir + "/graphchi/";
  }
  if (FLAGS_naiad_dir == "") {
    FLAGS_naiad_dir = FLAGS_root_dir + "/Naiad/";
  }
  if (FLAGS_naiad_hosts_file == "") {
    FLAGS_naiad_hosts_file = FLAGS_root_dir + "/machines";
  }
  if (FLAGS_powergraph_dir == "") {
    FLAGS_powergraph_dir = FLAGS_root_dir + "/graphlab/";
  }
  if (FLAGS_powerlyra_dir == "") {
    FLAGS_powerlyra_dir = FLAGS_root_dir + "/powerlyra/";
  }
  if (FLAGS_spark_dir == "") {
    FLAGS_spark_dir = FLAGS_root_dir + "/spark-0.9.0-incubating/";
  }

  int port = atoi(FLAGS_daemon_port.c_str());
  HistoryStorage* history = new HistoryStorage();
  map<string, FrameworkInterface* > frameworks =
    AddFrameworks(FLAGS_use_frameworks);
  /*
  if (FLAGS_optimise_ir_dag == true) {
    optimiser = new QueryOptimiser(true, history);
  } else {
    optimiser = new QueryOptimiser(false, history);
  }
  */
  SchedulerInterface* scheduler =
    new SchedulerDynamic(frameworks, history);
  //     SchedulerInterface* scheduler =  new OperatorScheduler(frameworks);
  if (FLAGS_run_daemon) {
    boost::thread daemon(set_up_daemon, port, scheduler);
  }
  while (true) {
    pANTLR3_INPUT_STREAM input;
    pRLPlusLexer lexer;
    pANTLR3_COMMON_TOKEN_STREAM tokens;
    pRLPlusParser parser;
    LOG(INFO) << "Looking for new Job to schedule";
    Job* job;
    if (FLAGS_run_daemon) {
      job = scheduler->GetJobFromQueue();
      FLAGS_force_framework = job->force_framework();
      // TODO(ionel): Fix job submit. It currently doesn't send the
      // operator merge flag.
      job->set_operator_merge("1");
      LOG(INFO) << "Job found " << job->code().c_str();
    } else {
      if (FLAGS_beer_query == "") {
        LOG(FATAL) << "No input file specified (-i)!";
        return 0;
      }
      job = new Job();
      job->set_force_framework(FLAGS_force_framework.c_str());
      job->set_frameworks(FLAGS_use_frameworks.c_str());
      if (FLAGS_operator_merge) {
        job->set_operator_merge("1");
      } else {
        job->set_operator_merge("0");
      }
      job->set_code(FLAGS_beer_query.c_str());
    }
    if (FLAGS_beer_query.find(".gr") != std::string::npos) {
      beeraph::BeeraphTranslator* beerraph = new beeraph::BeeraphTranslator();
      string code = FLAGS_beer_query;
      beerraph->translateToBeer(code);
      job->set_code("beeraph.rap");
    }
    input = antlr3AsciiFileStreamNew((pANTLR3_UINT8)job->code().c_str());
    lexer = RLPlusLexerNew(input);
    tokens = antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT,
                                              TOKENSOURCE(lexer));
    parser = RLPlusParserNew(tokens);
    RLPlusParser_expr_return expr_ret = parser->expr(parser);
    TreeTraversal tree_traversal = TreeTraversal(expr_ret.tree);
    vector<shared_ptr<OperatorNode> > dag = tree_traversal.Traverse();
    if (FLAGS_output_ir_dag_gv) {
      PrintDagGV(dag);
    } else {
      PrintDag(dag);
    }
    if (!strcmp(job->operator_merge().c_str(), "1")) {
      LOG(INFO) << "Scheduling entire DAG";
      if (FLAGS_use_dynamic_scheduler) {
        scheduler->DynamicScheduleDAG(dag);
      } else {
        scheduler->ScheduleDAG(dag);
      }
    } else {
      LOG(INFO) << "Individual Operator Scheduling";
      for (vector<shared_ptr<OperatorNode> >::iterator it = dag.begin();
           it != dag.end(); ++it) {
        vector<shared_ptr<OperatorNode> > op_dag;
        op_dag.push_back(*it);
        if (FLAGS_use_dynamic_scheduler) {
          scheduler->DynamicScheduleDAG(op_dag);
        } else {
          scheduler->ScheduleDAG(op_dag);
        }
      }
    }
    delete job;
    parser->free(parser);
    tokens->free(tokens);
    lexer->free(lexer);
    input->close(input);
    LOG(INFO) << "Finished scheduling job";
    // We're running in no daemon mode.
    if (!FLAGS_run_daemon) {
      return 0;
    }
  }
  return 0;
}
