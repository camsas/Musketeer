      if (this.is_left_rel) {
        String {{LEFT_REL}}_tmp = "";
        String left_join_cols = "";
        for (int {{LEFT_REL}}_i = 0; {{LEFT_REL}}_i < {{LEFT_REL}}.length; {{LEFT_REL}}_i++) {
          {{LEFT_REL}}_tmp += " " + {{LEFT_REL}}[{{LEFT_REL}}_i];
          if ({{#LEFT_INDICES}}{{CHECK_INDEX}}{{#LEFT_INDICES_separator}} || {{/LEFT_INDICES_separator}}{{/LEFT_INDICES}}) {
            left_join_cols += {{LEFT_REL}}[{{LEFT_REL}}_i] + " ";
          }
        }
        context.write(new Text(left_join_cols),
                      new Text("L" + {{LEFT_REL}}_tmp));
      } else {
        String {{RIGHT_REL}}_tmp = "";
        String right_join_cols = "";
        for (int {{RIGHT_REL}}_i = 0; {{RIGHT_REL}}_i < {{RIGHT_REL}}.length; {{RIGHT_REL}}_i++) {
          if ({{#RIGHT_INDICES}}{{CHECK_NOT_INDEX}}{{#RIGHT_INDICES_separator}} && {{/RIGHT_INDICES_separator}}{{/RIGHT_INDICES}}) {
            {{RIGHT_REL}}_tmp += " " + {{RIGHT_REL}}[{{RIGHT_REL}}_i];
          } else {
            right_join_cols += {{RIGHT_REL}}[{{RIGHT_REL}}_i] + " ";
          }
        }
        if ({{RIGHT_REL}}_tmp.isEmpty()) {
          {{RIGHT_REL}}_tmp = " ";
        }
        context.write(new Text(right_join_cols),
                      new Text("R" + {{RIGHT_REL}}_tmp));
      }
