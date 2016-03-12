      {{MAX_VALUE}}
      String maxRow = "";
      for (Text text : values) {
        String[] row = text.toString().split(" ");
        {{COL_TYPE}} value = {{COL_TYPE}}.valueOf(row[0]);
        if (maxValue.compareTo(value) < 0) {
          maxValue = value;
          maxRow = text.toString();
        }
      }
      String {{OUTPUT_REL}}_tmp = key + " " + maxRow;
      String[] {{OUTPUT_REL}} = {{OUTPUT_REL}}_tmp.split(" ");
      {{OUTPUT_CODE}}
      {{NEXT_OPERATOR}}
