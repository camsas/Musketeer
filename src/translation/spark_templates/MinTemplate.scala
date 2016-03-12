val int_{{OUTPUT}} = {{CONDITION}}.reduce((e1,e2) =>
   if (e1._{{COL_INDEX}} < e2._{{COL_INDEX}}) e1 else e2)
val res_{{OUTPUT}} = Seq(("ALL", {{OUTPUT_COLS}}))
{{OUTPUT}} = sc.parallelize((res_{{OUTPUT}})){{TO_CACHE}}

