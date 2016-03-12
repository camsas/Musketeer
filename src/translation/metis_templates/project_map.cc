        std::vector<std::pair<std::vector<const char*>, uint32_t> > {{OUTPUT_REL}};
        char tmpbuf[TEMP_BUF_SIZE];
        if (!rel_{{REL_NAME}}.empty()) {
          for (std::vector<std::pair<std::vector<const char*>, uint32_t> >::iterator row_it = rel_{{REL_NAME}}.begin();
               row_it != rel_{{REL_NAME}}.end(); ++row_it) {
            if ({{CONDITION}}) {
              std::vector<const char*> row(1);  // XXX
              uint32_t row_length = 0;
              uint32_t i = 0;
              for (std::vector<const char*>::iterator col_it = row_it->first.begin();
                   col_it != row_it->first.end();
                   ++i, ++col_it) {
                if ((1 << i) & {{COLUMN_MASK}}) {
                  row[i] = col_it[i];
                  row_length += strlen(col_it[i]);
                  //row_length += sprintf(tmpbuf, "%ld", col_it[i]);
                }
              }
              {{OUTPUT_REL}}.push_back(std::make_pair(row, row_length));
            }
          }
          VLOG(2) << "{{OUTPUT_REL}} of " << {{OUTPUT_REL}}.size() << " rows after project on {{REL_NAME}}";
        }
        {{NEXT_OPERATOR}}
        {{OUTPUT_CODE}}

