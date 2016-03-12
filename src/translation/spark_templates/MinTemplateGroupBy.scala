val keyed_{{CLASS_NAME}}:org.apache.spark.rdd.RDD[({{GROUP_BY}}, {{INPUTREL_TYPE}})] =
  {{CONDITION}}.{{GROUP_BY_KEY}}
val int_{{OUTPUT}} = keyed_{{CLASS_NAME}}.reduceByKey((e1,e2) => if (e1{{COL_INDEX}} > e2{{COL_INDEX}}) e2 else e1)
{{OUTPUT}} = int_{{OUTPUT}}.map(({{INPUT}} => { {{NEXT_OPERATOR}} })){{TO_CACHE}}

