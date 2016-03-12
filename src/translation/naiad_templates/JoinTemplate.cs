{{OUTPUT_REL}} = {{LEFT_REL}}.Join({{RIGHT_REL}},
                        ({{LEFT_TYPE}} left) => left.{{LEFT_INDEX}},
                        ({{RIGHT_TYPE}} right) => right.{{RIGHT_INDEX}},
                        ({{LEFT_TYPE}} left, {{RIGHT_TYPE}} right) => {
                          {{NEXT_OPERATOR}} });
