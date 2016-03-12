        String {{LEFT_REL}}_tmp1 = "";
        for (int {{LEFT_REL}}_i = 0; {{LEFT_REL}}_i < {{LEFT_REL}}.length; {{LEFT_REL}}_i++) {
          {{LEFT_REL}}_tmp1 += " " + {{LEFT_REL}}[{{LEFT_REL}}_i];
        }
        context.write(new Text({{LEFT_REL}}[{{LEFT_INDEX}}]),
                      new Text("L" + {{LEFT_REL}}_tmp1));
        String {{RIGHT_REL}}_tmp2 = "";
        for (int {{RIGHT_REL}}_i = 0; {{RIGHT_REL}}_i < {{RIGHT_REL}}.length; {{RIGHT_REL}}_i++) {
          if ({{RIGHT_REL}}_i != {{RIGHT_INDEX}}) {
            {{RIGHT_REL}}_tmp2 += " " + {{RIGHT_REL}}[{{RIGHT_REL}}_i];
          }
        }
        if ({{RIGHT_REL}}_tmp2.isEmpty()) {
          {{RIGHT_REL}}_tmp2 = " ";
        }
        context.write(new Text({{RIGHT_REL}}[{{RIGHT_INDEX}}]),
                      new Text("R" + {{RIGHT_REL}}_tmp2));
