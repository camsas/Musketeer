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

#ifndef MUSKETEER_FLAGS_H
#define MUSKETEER_FLAGS_H

#include "base/common.h"

DECLARE_string(i);
DECLARE_string(root_dir);
DECLARE_string(include_frameworks);
DECLARE_string(force_framework);
DECLARE_string(daemon_port);
DECLARE_bool(optimise_ir_dag);
DECLARE_bool(run_daemon);
DECLARE_bool(output_ir_dag_gv);
DECLARE_string(tmp_data_dir);
DECLARE_string(generated_code_dir);
DECLARE_string(hdfs_input_dir);

// Scheduler flags.
DECLARE_bool(operator_merge);
DECLARE_bool(dry_run);
DECLARE_double(time_to_cost);
DECLARE_string(dry_run_data_size_file);
DECLARE_bool(populate_history);
DECLARE_uint64(max_scheduler_cost);
DECLARE_bool(best_runtime);
DECLARE_bool(use_heuristic);
DECLARE_bool(use_dynamic_scheduler);

// HDFS flags.
DECLARE_string(hdfs_master);
DECLARE_string(hdfs_port);

// GraphChi flags.
DECLARE_string(graphchi_dir);
DECLARE_string(graphchi_templates_dir);

// Hadoop flags.
DECLARE_string(hadoop_templates_dir);
DECLARE_string(hadoop_job_tracker_host);
DECLARE_int32(hadoop_job_tracker_port);

// Metis flags.
DECLARE_string(metis_dir);
DECLARE_string(metis_templates_dir);
DECLARE_bool(metis_use_hdfs);
DECLARE_bool(metis_debug_binaries);
DECLARE_bool(metis_strace_binaries);
DECLARE_bool(metis_heapprofile);
DECLARE_bool(metis_cpuprofile);
DECLARE_string(metis_glog_v);

// Naiad flag.s
DECLARE_string(naiad_dir);
DECLARE_string(naiad_templates_dir);
DECLARE_int32(naiad_num_workers);
DECLARE_int32(naiad_num_threads);
DECLARE_string(naiad_hosts_file);

// PowerGraph flags.
DECLARE_string(powergraph_dir);
DECLARE_string(powergraph_templates_dir);
DECLARE_int32(powergraph_num_workers);

// Spark flags.
DECLARE_string(spark_dir);
DECLARE_string(spark_master);
DECLARE_string(spark_templates_dir);
DECLARE_string(spark_version);
DECLARE_string(scala_version);
DECLARE_string(scala_major_version);
DECLARE_string(spark_web_ui_host);
DECLARE_int32(spark_web_ui_port);

// WildCherry flags.
DECLARE_string(wildcherry_dir);
DECLARE_string(wildcherry_loop_its);
DECLARE_string(wildcherry_templates_dir);
DECLARE_bool(wildcherry_use_hdfs);
#endif
