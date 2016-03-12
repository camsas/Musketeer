#ifndef METIS_GENERATED_HDFS_UTILS_H
#define METIS_GENERATED_HFDS_UTILS_H

#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

#include "jni.h"
#include "hdfs.h"
#include "utils.h"

// Metis types
#include "mr-types.hh"

// Glog logging
#include <glog/logging.h>

bool copyFileFromHDFS(hdfsFS distfs, const char* hdfs_filename, const char* local_filename) {
  hdfsFile hdfsOutFD = NULL;
  hdfsFS localfs = hdfsConnect(NULL, 0);
  if (hdfsCopy(distfs, hdfs_filename, localfs, local_filename) != 0) {
     LOG(ERROR) << "failed to copy input from " << hdfs_filename
                << " to " << local_filename << "!";
     return false;
  }
  /* clean up HDFS state */
  hdfsDisconnect(localfs);
  return true;
}

bool copyAndMergeFilesFromHDFSNoPrefix(std::vector<std::string>& files,
                                       const char* local_merged_filename) {
  hdfsFS distfs = hdfsConnect("freestyle.private.srg.cl.cam.ac.uk", 8020);  // XXX
  /* open and merge the input files into one big local file */
  FILE* localInFD = fopen(local_merged_filename, "wb");
  uint32_t input_id = 0;
  VLOG(2) << "Local merged file name: " << local_merged_filename;
  for (std::vector<std::string>::const_iterator path_it = files.begin();
       path_it != files.end();
       ++path_it) {
    int32_t num_entries = 0;  // libHDFS expects a signed integer
    hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_it->c_str(), &num_entries);
    VLOG(1) << "NEXT INPUT: " << *path_it;
    // Fast-path in the case of having only one file
    if (num_entries == 1) {
      VLOG(1) << "Only one input, so taking fast-path via hdfsCopy!";
      return copyFileFromHDFS(distfs, hdfs_info[0].mName, local_merged_filename);
    }
    // Otherwise we need to merge
    for (uint32_t entry_id = 0; entry_id < num_entries; ++entry_id) {
      if (hdfs_info[entry_id].mKind == kObjectKindDirectory) {
        continue;
      }
      hdfsFile hdfsInFD = hdfsOpenFile(distfs, hdfs_info[entry_id].mName, O_RDONLY, 0, 0, 0);
      VLOG(2) << "hdfsOpen of " << hdfs_info[entry_id].mName << ": "
              << ((hdfsInFD == NULL) ? "FAILED" : "SUCCEEDED");
      if (!hdfsInFD) {
        LOG(FATAL) << "Failed to open input file on HDFS!";
        return false;
      }
      tSize bytes_read = 1;
      uint32_t buflen = 1 << 12;  // 4K
      char buf[buflen];
      size_t written_up_to = 0;
      size_t read_up_to = 0;
      bool initial = true;
      while (bytes_read > 0) {
        VLOG(3) << "Watermarks BEGIN: read_up_to " << read_up_to << " bytes, "
                << "written_up_to " << written_up_to << " bytes.";
        bytes_read = hdfsRead(distfs, hdfsInFD, &buf[read_up_to], buflen - read_up_to);
        read_up_to += bytes_read;
        // Check if there are any newlines in the data read, and synthesize
        // input file IDs in the merged local file.
        size_t read_limit = read_up_to - written_up_to;
        VLOG(3) << "read " << bytes_read << " bytes, read_up_to now "
                << read_up_to << ", read_limit " << read_limit;
        // Write out what we've got so far
        size_t bytes_written = 0;
        bytes_written = fwrite(&buf[written_up_to], 1, read_up_to - written_up_to, localInFD);
        // Remember offset for next write
        written_up_to += bytes_written;
        VLOG(3) << "wrote " << bytes_written << " bytes, written_up_to now "
                << written_up_to;
        VLOG(3) << "watermarks END: read_up_to " << read_up_to << " bytes, "
                << "written_up_to " << written_up_to << " bytes";
        if (written_up_to > 0 && written_up_to < read_up_to) {
          VLOG(3) << "memmove from " << written_up_to << " to 0, length of moved region " << (read_up_to - written_up_to);
          memmove(&buf[0], &buf[written_up_to], read_up_to - written_up_to);
          read_up_to -= written_up_to;
          written_up_to = 0;
          VLOG(3) << "read_up_to: " << read_up_to << ", written_up_to: " << written_up_to;
        } else if (written_up_to == buflen) {
          VLOG(3) << "buffer written entirely, no memmove required";
          written_up_to = 0;
          read_up_to = 0;
        }
      }
      // Write out any remainder
      fwrite(&buf[written_up_to], 1, read_up_to - written_up_to, localInFD);
      if (buf[read_up_to-1] != '\n')
        fprintf(localInFD, "\n");
      VLOG(2) << "Done, closing HDFS file";
      hdfsCloseFile(distfs, hdfsInFD);
    }
    input_id++;
  }
  fclose(localInFD);
  /* clean up HDFS state */
  hdfsDisconnect(distfs);
  return true;
}


bool copyAndMergeFilesFromHDFS(std::vector<std::string>& files, const char* local_merged_filename) {
  hdfsFS distfs = hdfsConnect("freestyle.private.srg.cl.cam.ac.uk", 8020);  // XXX
  /* open and merge the input files into one big local file */
  FILE* localInFD = fopen(local_merged_filename, "wb");
  uint32_t input_id = 0;
  VLOG(2) << "Local merged file name: " << local_merged_filename;
  for (std::vector<std::string>::const_iterator path_it = files.begin();
       path_it != files.end();
       ++path_it) {
    int32_t num_entries = 0;  // libHDFS expects a signed integer
    hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_it->c_str(), &num_entries);
    VLOG(1) << "NEXT INPUT: " << *path_it;
    for (uint32_t entry_id = 0; entry_id < num_entries; ++entry_id) {
      if (hdfs_info[entry_id].mKind == kObjectKindDirectory) {
        continue;
      }
      hdfsFile hdfsInFD = hdfsOpenFile(distfs, hdfs_info[entry_id].mName, O_RDONLY, 0, 0, 0);
      VLOG(2) << "hdfsOpen of " << hdfs_info[entry_id].mName << ": "
              << ((hdfsInFD == NULL) ? "FAILED" : "SUCCEEDED");
      if (!hdfsInFD) {
        LOG(FATAL) << "Failed to open input file on HDFS!";
        return false;
      }
      tSize bytes_read = 1;
      uint32_t buflen = 1 << 12;  // 4K
      char buf[buflen];
      size_t written_up_to = 0;
      size_t read_up_to = 0;
      bool initial = true;
      while (bytes_read > 0) {
        VLOG(3) << "Watermarks BEGIN: read_up_to " << read_up_to << " bytes, "
                << "written_up_to " << written_up_to << " bytes.";
        bytes_read = hdfsRead(distfs, hdfsInFD, &buf[read_up_to], buflen - read_up_to);
        read_up_to += bytes_read;
        // Check if there are any newlines in the data read, and synthesize
        // input file IDs in the merged local file.
        size_t read_limit = read_up_to - written_up_to;
        VLOG(3) << "read " << bytes_read << " bytes, read_up_to now "
                << read_up_to << ", read_limit " << read_limit;
        if (initial) {
          VLOG(3) << "emitting initial newline";
          fprintf(localInFD, "%u ", input_id);
          initial = false;
        }
        uint32_t last_newline_idx = 0;
        for (uint32_t j = 1; j < read_limit; ++j) {
          //VLOG(3) << "checking char at offset " << j << " - 1";
          if (buf[j-1] == '\n') {
            VLOG(3) << "found newline at offset " << (j - 1);
            // Write out what we've got so far
            size_t bytes_written = 0;
            bytes_written = fwrite(&buf[written_up_to], 1, j - last_newline_idx, localInFD);
            if (written_up_to + bytes_written < read_up_to || hdfsAvailable(distfs, hdfsInFD) > 0) {
              // Need to synthesize a prefix to the new line
              int result = fprintf(localInFD, "%u ", input_id);
              assert(result > 0);
            }
            // Remember offset for next write
            written_up_to += bytes_written;
            VLOG(3) << "wrote " << bytes_written << " bytes, written_up_to now "
                    << written_up_to << ", j: " << j << ", last_newline_idx: "
                    << last_newline_idx;
            last_newline_idx = j;
          }
        }
        VLOG(3) << "watermarks END: read_up_to " << read_up_to << " bytes, "
                << "written_up_to " << written_up_to << " bytes";
        if (written_up_to > 0 && written_up_to < read_up_to) {
          VLOG(3) << "memmove from " << written_up_to << " to 0, length of moved region "
                  << (read_up_to - written_up_to);
          memmove(&buf[0], &buf[written_up_to], read_up_to - written_up_to);
          read_up_to -= written_up_to;
          written_up_to = 0;
          VLOG(3) << "read_up_to: " << read_up_to << ", written_up_to: " << written_up_to;
        } else if (written_up_to == buflen) {
          VLOG(3) << "buffer written entirely, no memmove required";
          written_up_to = 0;
          read_up_to = 0;
        }
      }
      // Write out any remainder
      fwrite(&buf[written_up_to], 1, read_up_to - written_up_to, localInFD);
      if (buf[read_up_to-1] != '\n')
        fprintf(localInFD, "\n");
      VLOG(2) << "Done, closing HDFS file";
      hdfsCloseFile(distfs, hdfsInFD);
    }
    input_id++;
  }
  fclose(localInFD);
  /* clean up HDFS state */
  hdfsDisconnect(distfs);
  return true;
}

static void output_all_hdfs(xarray<keyval_t> *wc_vals, hdfsFS distfs,
                            hdfsFS localfs,
                            const char* local_out_temp_filename,
                            const char* hdfs_out_filename) {
  FILE* localOutFD = fopen(local_out_temp_filename, "w");
  if (!localOutFD) {
    PLOG(FATAL) << "unable to open " << local_out_temp_filename << ": ";
  }
  output_all_local(wc_vals, localOutFD);
  fclose(localOutFD);
  if (hdfsCopy(localfs, local_out_temp_filename, distfs, hdfs_out_filename) != 0) {
    LOG(ERROR) << "failed to copy output from " << local_out_temp_filename
               << " to " << hdfs_out_filename << " on HDFS!";
  }
}

bool writeResultsToHDFS(const char* output_filename, xarray<keyval_t>* results) {
  hdfsFile hdfsOutFD = NULL;
  hdfsFS distfs = hdfsConnect("freestyle.private.srg.cl.cam.ac.uk", 8020);  // XXX
  hdfsFS localfs = hdfsConnect(NULL, 0);
  output_all_hdfs(results, distfs, localfs, "/tmp/metis.tmp", output_filename);
  /* clean up HDFS state */
  hdfsDisconnect(localfs);
  hdfsDisconnect(distfs);
  return true;
}

#endif  // METIS_GENERATED_HDFS_UTILS_H
