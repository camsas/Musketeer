      {{TYPE}} aggValues[] = new {{TYPE}}[{{NUM_AGG_COLS}}];
      for (int i = 0; i < aggValues.length; ++i) {
        aggValues[i] = {{TYPE}}.valueOf("{{INIT_VAL}}");
      }
      for (Text text : values) {
        String[] cols = text.toString().split(" ");
        for (int i = 0; i < aggValues.length; ++i) {
          aggValues[i] {{OPERATOR}}= {{TYPE}}.valueOf(cols[i]);
        }
      }
      String {{OUTPUT_REL}}_tmp = key.toString();
      for (int i = 0; i < aggValues.length; ++i) {
          {{OUTPUT_REL}}_tmp += " " + String.valueOf(aggValues[i]);
      }
      String[] {{OUTPUT_REL}} = {{OUTPUT_REL}}_tmp.split(" ");
      {{NEXT_OPERATOR}}
      {{OUTPUT_CODE}}
