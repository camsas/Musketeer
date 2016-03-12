      // Temp input buffer
      char input_buf[TEMP_BUF_SIZE];
      size_t input_buf_len;
      split_lines sl(ma);
      // Output buffer; upper bound is the size of the input. This is a bit wasteful of memory,
      // the best we can do in order to avoid excessive small mallocs in the loop.
      char* scratch_space = safe_malloc<char>(ma->length); 
      uint64_t offset_filled_up_to = 0;
      uint64_t row_start = 0;
      uint64_t buffer_scale_factor = 1;
      while (sl.fill(input_buf, sizeof(input_buf), input_buf_len)) {
        // Check if we have enough space left in the buffer to hold the
        // entire input row; if not, we need to grow the buffer
        if ((offset_filled_up_to + 2*input_buf_len) > buffer_scale_factor * ma->length) {
          // Error condition; could re-alloc here for other operators
          // or better memory efficiency.
          VLOG(2) << "Increasing size of map output buffer from "
                  << (buffer_scale_factor * ma->length) << " to "
                  << ((buffer_scale_factor + 1) * ma->length) << " bytes." ;
          buffer_scale_factor++;
          scratch_space = safe_malloc<char>(ma->length);
          offset_filled_up_to = 0;
          row_start = 0;
        }
        char* tmpptr1 = NULL;
        VLOG(3) << "Processing row: " << input_buf;
        char* input_id_str = strtok_r(input_buf, " ", &tmpptr1);
        int32_t input_id = atoi(input_id_str);
        // Now split the row and insert into the appropriate relation
        int i = 0;
        char* piece = input_id_str;
        bool first = true;
        char key_tmp_buf[TEMP_BUF_SIZE];
        do {
          if (!first)
            scratch_space[offset_filled_up_to++] = ' ';
          strcpy(&scratch_space[offset_filled_up_to], piece);
          size_t len = strlen(piece);
          offset_filled_up_to += len;
          if (!first) {
            if (input_id == 0 && i == {{LEFT_INDEX}}) {
              strncpy(&key_tmp_buf[0], piece, TEMP_BUF_SIZE);
            } else if (input_id == 1 && i == {{RIGHT_INDEX}}) {
              strncpy(&key_tmp_buf[0], piece, TEMP_BUF_SIZE);
            }
            ++i;
          }
          if (first)
            first = false;
        } while ((piece = strtok_r(NULL, " ", &tmpptr1)) != NULL);
        // Put null terminator as end-of-row sign
        scratch_space[offset_filled_up_to++] = '\0';
        // Copy the key into the buffer just behind the data
        strcpy(&scratch_space[offset_filled_up_to], &key_tmp_buf[0]);
        char* key = &scratch_space[offset_filled_up_to];
        offset_filled_up_to += strlen(key_tmp_buf);
        scratch_space[offset_filled_up_to++] = '\0';
        if (strlen(key) == 0 || strlen(scratch_space + row_start) == 0)
          VLOG(3) << "Emit key " << key << ", row " << (scratch_space + row_start);
        map_emit((void*)key, (void*)(scratch_space + row_start), strlen(key));
        row_start = offset_filled_up_to;
      }
      // Truncate the buffer for memory efficiency
      /*if (offset_filled_up_to > 0) {
        if (realloc(scratch_space, offset_filled_up_to) == 0)
          LOG(FATAL) << "Failed to truncate map output buffer!";
      }*/
