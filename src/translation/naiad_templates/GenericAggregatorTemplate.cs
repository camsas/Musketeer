var {{OUTPUT_REL}}_tmp = {{INPUT_REL}}{{WHERE}}.GenericAggregator(row => {{KEY}},
                                                                  row => {{VAL_SELECTOR}});
{{OUTPUT_REL}} = {{OUTPUT_REL}}_tmp.Select(row => { {{NEXT_OPERATOR}} });
