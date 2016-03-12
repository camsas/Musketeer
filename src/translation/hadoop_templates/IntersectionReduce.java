      BooleanWritable true_bool = new BooleanWritable(true);
      int cnt = 0;
      boolean right_rel = false;
      for (BooleanWritable value : values) {
        if (value.equals(true_bool)) {
          cnt++;
        } else {
          right_rel = true;
        }
      }
      if (right_rel) {
        for (; cnt > 0; cnt--) {
          String[] {{OUTPUT_REL}} = key.toString().trim().split(" ");
          {{NEXT_OPERATOR}}
          {{OUTPUT_CODE}}
        }
      }
