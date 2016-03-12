          for (std::vector<std::pair<std::vector<const char*>, uint32_t> >::const_iterator out_row_it = {{OUTPUT_REL}}.begin();
               out_row_it != {{OUTPUT_REL}}.end();
               ++out_row_it) {
            bool first = true;
            std::string* out_row = new std::string;
            for (std::vector<const char*>::const_iterator out_col_it = out_row_it->first.begin();
                 out_col_it != out_row_it->first.end();
                 ++out_col_it) {
                if (!first) {
                  out_row->append(" ");
                } else {
                  first = false;
                }
                out_row->append(*out_col_it);
            }
            reduce_emit(key_in, (void *)out_row->c_str());
            VLOG(2) << "out_row is " << *out_row;
          }

