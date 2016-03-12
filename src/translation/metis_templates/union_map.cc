        std::vector<std::pair<std::vector<const char*>, uint32_t> > {{OUTPUT_REL}};
        if (!rel_{{LEFT_REL}}.empty()) {
          for (std::vector<std::pair<std::vector<const char*>, uint32_t> >::iterator row_it =
            rel_{{LEFT_REL}}.begin();
               row_it != rel_{{LEFT_REL}}.end(); ++row_it) {
            std::vector<const char*> row;
            uint64_t row_length = 0;
            for (std::vector<const char*>::const_iterator col_it = row_it->first.begin();
                 col_it != row_it->first.end();
                 ++col_it) {
                row.push_back(*col_it);
                row_length += strlen(*col_it) + 1;
            }
            {{OUTPUT_REL}}.push_back(std::make_pair(row, row_length));
          }
        }
        if (!rel_{{RIGHT_REL}}.empty()) {
          for (std::vector<std::pair<std::vector<const char*>, uint32_t> >::iterator row_it =
            rel_{{RIGHT_REL}}.begin();
               row_it != rel_{{RIGHT_REL}}.end(); ++row_it) {
            std::vector<const char*> row;
            uint64_t row_length = 0;
            for (std::vector<const char*>::const_iterator col_it = row_it->first.begin();
                 col_it != row_it->first.end();
                 ++col_it) {
                row.push_back(*col_it);
                row_length += strlen(*col_it) + 1;
            }
            {{OUTPUT_REL}}.push_back(std::make_pair(row, row_length));
          }
        }
        {{NEXT_OPERATOR}}
        {{OUTPUT_CODE}}

