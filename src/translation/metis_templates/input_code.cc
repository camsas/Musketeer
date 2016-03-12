      // Input relations
      std::map<const char*, uint16_t> input_name_to_cols;
      std::map<uint32_t, uint16_t> input_id_to_cols;
      {{#INPUT_RELATIONS}}
      // Relation {{REL_NAME}} with {{NUM_COLS}} columns
      std::vector<std::pair<std::vector<const char*>, uint32_t> > rel_{{REL_NAME}};
      input_name_to_cols["{{REL_NAME}}"] = {{NUM_COLS}};
      input_id_to_cols[{{REL_ID}}] = {{NUM_COLS}};
      {{/INPUT_RELATIONS}}
      // Temp buffers
      char input_buf[TEMP_BUF_SIZE];
      size_t input_buf_len;
      split_lines sl(ma);
      // Extract all rows (leaving columns space-separated for now)
      while (sl.fill(input_buf, sizeof(input_buf), input_buf_len)) {
        char* tmpptr1 = NULL;
        char* input_id_str = strtok_r(input_buf, " ", &tmpptr1);
        int32_t input_id = atoi(input_id_str);
        char* row_str = safe_malloc<char>(input_buf_len - strlen(input_id_str));
        VLOG(3) << "malloc'd at " << std::hex << (void*)row_str;
        memcpy(row_str, input_buf + strlen(input_id_str) + 1,
               input_buf_len - (strlen(input_id_str) + 1));
        row_str[input_buf_len - (strlen(input_id_str) + 1)] = '\0';
        VLOG(3) << "found " << row_str << " (len: " << input_buf_len << ")";
        // Now split the row and insert into the appropriate relation
        int i = 0;
        uint32_t row_length = 0;
        VLOG(3) << "Processing row: " << row_str;
        char* tmpptr2 = NULL;
        char* piece = strtok_r(row_str, " ", &tmpptr2);
        std::vector<const char*> row(input_id_to_cols[input_id]);
        do {
          row[i] = piece;
          row_length += strlen(piece);
          ++i;
        } while ((piece = strtok_r(NULL, " ", &tmpptr2)) != NULL);
        assert(input_id >= 0);
        {{#INPUT_RELATIONS}}
        if (input_id == {{REL_ID}})
          rel_{{REL_NAME}}.push_back(make_pair(row, row_length));
        {{/INPUT_RELATIONS}}
        //free((char*)row_str);
      }

      {{#INPUT_RELATIONS}}
      VLOG(3) << rel_{{REL_NAME}}.size()
              << " rows for input relation {{REL_NAME}}.";
      {{/INPUT_RELATIONS}}

