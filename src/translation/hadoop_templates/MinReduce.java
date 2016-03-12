      {{MIN_VALUE}}
      String minRow = "";
      for (Text text : values) {
        String[] row = text.toString().split(" ");
        {{COL_TYPE}} value = {{COL_TYPE}}.valueOf(row[0]);
        if (minValue.compareTo(value) > 0 || minValue.equals("")) {
          minValue = value;
          minRow = text.toString();
        }
      }
      String {{OUTPUT_REL}}_tmp = key + " " + minRow;
      String[] {{OUTPUT_REL}} = {{OUTPUT_REL}}_tmp.split(" ");
      {{OUTPUT_CODE}}
      {{NEXT_OPERATOR}}
