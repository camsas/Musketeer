val int_{{OUTPUT}} = {{CONDITION}}.reduce((e1,e2) => {{AGG}})
val res_{{OUTPUT}} = Seq(("ALL", {{OUTPUT_COLS}}));
{{OUTPUT}} = sc.parallelize((res_{{OUTPUT}})){{TO_CACHE}};

