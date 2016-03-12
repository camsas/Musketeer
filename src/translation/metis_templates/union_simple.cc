      // Temp input buffer
      char input_buf[TEMP_BUF_SIZE];
      size_t input_buf_len;
      split_lines sl(ma);
      // Output buffer; upper bound is the size of the input. This is a bit wasteful of memory,
      // the best we can do in order to avoid excessive small mallocs in the loop.
      char* scratch_space = safe_malloc<char>(ma->length); 
      uint64_t offset_filled_up_to = 0;
      uint64_t row_start = 0;
      // Extract all rows (leaving columns space-separated for now)
      while (sl.fill(input_buf, sizeof(input_buf), input_buf_len)) {
        // Check if we have enough space left in the buffer to hold the
        // entire input row; if not, we're producing more data than we
        // consume, which is an error for our UNION implementation (as it
        // already merges the inputs before running the operator code)
        if ((offset_filled_up_to + input_buf_len) > ma->length) {
          // Error condition; could re-alloc here for other operators
          // or better memory efficiency.
          LOG(FATAL) << "Ran out of output buffer space!";
        }
        char* tmpptr1 = NULL;
        char* input_id_str = strtok_r(input_buf, " ", &tmpptr1);
        int32_t input_id = atoi(input_id_str);
        // Now split the row and insert into the appropriate relation
        int i = 0;
        //VLOG(3) << "Processing row: " << input_id_str + strlen(input_id_str) + 1;
        char* piece = input_id_str + strlen(input_id_str) + 1;
        bool first = true;
        strcpy(&scratch_space[offset_filled_up_to], piece);
        size_t len = strlen(piece);
        offset_filled_up_to += len;
        // Put null terminator as end-of-row sign
        scratch_space[offset_filled_up_to++] = '\0';
        map_emit((void*)(scratch_space + row_start), (void*)(scratch_space + row_start), offset_filled_up_to - row_start);
        row_start = offset_filled_up_to;
      }
      // Truncate the buffer for memory efficiency
      if (offset_filled_up_to > 0) {
        if (realloc(scratch_space, offset_filled_up_to) == 0)
          LOG(FATAL) << "Failed to truncate output buffer!";
      }
