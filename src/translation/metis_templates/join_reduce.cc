      VLOG(2) << "In reduce for key " << (char*)key_in;
      std::vector<const char*> arrayLeft;
      std::vector<const char*> arrayRight;
      for (uint64_t i = 0; i < vals_len; ++i) {
        const char* tmp = (const char*)vals_in[i];
        if (tmp[0] == '0') {
          arrayLeft.push_back(&tmp[2]);
        } else if (tmp[0] == '1') {
          arrayRight.push_back(&tmp[2]);
        } else {
          LOG(FATAL) << "Unrecognized input: " << tmp;
        }
      }
      VLOG(2) << "reduce for key " << (char*)key_in << " has " << arrayLeft.size()
              << " left elements and " << arrayRight.size() << " right ones.";
      std::vector<std::pair<std::vector<const char*>, uint32_t> > {{OUTPUT_REL}};
      for (std::vector<const char*>::const_iterator l_it = arrayLeft.begin();
           l_it != arrayLeft.end();
           ++l_it) {
        for (std::vector<const char*>::const_iterator r_it = arrayRight.begin();
             r_it != arrayRight.end();
             ++r_it) {
          VLOG(2) << "right row is " << *r_it;
          std::vector<const char*> row;
          std::vector<std::string> rightBuf = str_split(std::string(*r_it), ' ');
          std::string right_rel = "";
          for (int {{OUTPUT_REL}}_k = 0; {{OUTPUT_REL}}_k < rightBuf.size(); {{OUTPUT_REL}}_k++) {
            if ({{OUTPUT_REL}}_k != {{RIGHT_INDEX}}) {
              right_rel += " " + rightBuf[{{OUTPUT_REL}}_k];
            }
          }
          VLOG(2) << "right_rel: " << right_rel << ", l_it: " << *l_it;
          VLOG(2) << "pushing row " << (*l_it + right_rel);
          std::vector<std::string> {{OUTPUT_REL}}_tmp = str_split((std::string(*l_it) + right_rel), ' ');
          uint64_t row_length = 0;
          for (std::vector<std::string>::const_iterator col_it = {{OUTPUT_REL}}_tmp.begin();
               col_it != {{OUTPUT_REL}}_tmp.end();
               ++col_it) {
            std::string* tmp_str = new std::string(*col_it);
            row.push_back(tmp_str->c_str());
            row_length += tmp_str->size();
          }
          {{OUTPUT_REL}}.push_back(std::make_pair(row, row_length));
        }
      }
      {{NEXT_OPERATOR}}
      {{OUTPUT_CODE}}
