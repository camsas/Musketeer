          VLOG(3) << {{OUTPUT_REL}}.size() << " total output rows!";
          for (std::vector<std::pair<std::vector<const char*>, uint32_t> >::const_iterator out_row_it = {{OUTPUT_REL}}.begin();
               out_row_it != {{OUTPUT_REL}}.end();
               ++out_row_it) {
            bool first = true;
            VLOG(3) << "allocated " << (out_row_it->second + 1) << " bytes of memory for output row";
            char* out_row = safe_malloc<char>(out_row_it->second + 1);
            out_row[out_row_it->second] = '\0';
            uint32_t next_offset = 0;
            uint32_t i = 0;
            for (std::vector<const char*>::const_iterator out_col_it = out_row_it->first.begin();
                 out_col_it != out_row_it->first.end();
                 ++out_col_it) {
                if (!first) {
                  out_row[next_offset++] = ' ';
                } else {
                  first = false;
                }
                size_t len = strlen(*out_col_it);
                VLOG(3) << "inserting " << len << " bytes at offset " << next_offset << " of " << (out_row_it->second + 1) << " bytes row";
                assert(next_offset + len < out_row_it->second + 1);
                strcpy(&out_row[next_offset], *out_col_it);
                next_offset += len;
                //next_offset += sprintf(&out_row[next_offset], "%ld", out_col_it[i]);
            } 
            map_emit((void *)out_row, (void *)out_row, out_row_it->second);
          }

