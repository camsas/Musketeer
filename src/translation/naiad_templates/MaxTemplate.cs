{{OUTPUT_REL}} = {{INPUT_REL}}{{WHERE}}.GroupBy(row => {{KEY}},
                                                (key, selected) => {{FUN_NAME}}(key, selected));
