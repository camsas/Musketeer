        relation_t* {{OUTPUT_REL}} = new relation_t;
        if (!rel_{{REL_NAME}}.empty()) {
          for (relation_t::iterator row_it = rel_{{REL_NAME}}.begin();
               row_it != rel_{{REL_NAME}}.end(); ++row_it) {
            if ({{CONDITION}}) {
              uint32_t i = 0;
              row_t row;
              for (row_t::const_iterator col_it = row_it->begin();
                   col_it != row_it->end();
                   ++i, ++col_it) {
                if ((1 << i) & {{COLUMN_MASK}}) {
                  row.push_back(*col_it);
                }
              }
              {{OUTPUT_REL}}->push_back(row);
            }
          }
          VLOG(2) << "{{OUTPUT_REL}} of " << {{OUTPUT_REL}}->size() << " rows after project on {{REL_NAME}}";
        }
        {{NEXT_OPERATOR}}
        {{OUTPUT_CODE}}

