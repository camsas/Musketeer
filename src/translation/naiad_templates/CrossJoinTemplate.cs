{{OUTPUT_REL}} = {{LEFT_REL}}.Join({{RIGHT_REL}},
                        ({{LEFT_TYPE}} left) => 0,
                        ({{RIGHT_TYPE}} right) => 0,
                        ({{LEFT_TYPE}} left, {{RIGHT_TYPE}} right) => {
                          {{NEXT_OPERATOR}} });