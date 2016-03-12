      // No map operator can follow an Agg.
      // TODO(ionel): At the moment we only support Agg on multiple columns that
      // have the same type. FIX!
      if ({{CONDITION}}) {
        if ({{GROUP_BY}} == -1) {
          for (int i = 0; i < aggValues.length; i++) {
            {{TYPE}} col_val = {{TYPE}}.valueOf({{REL_NAME}}[indexArray[i]]);
            if (aggValues[i] == null) {
              aggValues[i] = col_val;
            } else {
              aggValues[i] = aggValues[i] {{OPERATOR}} col_val;
            }
          }
        } else {
          String outputValue = {{REL_NAME}}[indexArray[0]];
          for (int i = 1; i < aggValues.length; i++) {
            outputValue += " " + {{REL_NAME}}[indexArray[i]];
          }
          context.write(new Text({{GROUP_BY_KEY}}), new Text(outputValue));
        }
      }
