      if ({{CONDITION}}) {
        index = {{LEFT_COL_INDEX}} == -1 ? {{RIGHT_COL_INDEX}} : {{LEFT_COL_INDEX}};
        String[] {{OUTPUT_REL}} = new String[{{REL_NAME}}.length];
        for (int {{OUTPUT_REL}}_i = 0; {{OUTPUT_REL}}_i < {{REL_NAME}}.length; {{OUTPUT_REL}}_i++) {
          if ({{OUTPUT_REL}}_i == index) {
            if ({{LEFT_COL_INDEX}} == -1) {
              {{REL_NAME}}[{{OUTPUT_REL}}_i] = String.valueOf(
                {{RIGHT_TYPE}}.valueOf({{REL_NAME}}[{{RIGHT_COL_INDEX}}]) - {{VALUE}});
            } else {
              if ({{RIGHT_COL_INDEX}} == -1) {
                {{REL_NAME}}[{{OUTPUT_REL}}_i] = String.valueOf(
                  {{LEFT_TYPE}}.valueOf({{REL_NAME}}[{{LEFT_COL_INDEX}}]) - {{VALUE}});
              } else {
                {{REL_NAME}}[{{OUTPUT_REL}}_i] = String.valueOf(
                  {{LEFT_TYPE}}.valueOf({{REL_NAME}}[{{LEFT_COL_INDEX}}]) -
                  {{RIGHT_TYPE}}.valueOf({{REL_NAME}}[{{RIGHT_COL_INDEX}}]));
              }
            }
          }
          {{OUTPUT_REL}}[{{OUTPUT_REL}}_i] = {{REL_NAME}}[{{OUTPUT_REL}}_i];
        }
        {{NEXT_OPERATOR}}
        {{OUTPUT_CODE}}
      }
