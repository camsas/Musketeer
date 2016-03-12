      int cnt = 0;
      for (Text value : values) {
        cnt += Integer.valueOf(value.toString());
      }
      String {{OUTPUT_REL}}_tmp = key + " " + String.valueOf(cnt);
      String[] {{OUTPUT_REL}} = {{OUTPUT_REL}}_tmp.split(" ");
      {{NEXT_OPERATOR}}
      {{OUTPUT_CODE}}