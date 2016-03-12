val keyed_{{CLASS_NAME}}:org.apache.spark.rdd.RDD[({{GROUP_BY}}, {{INPUTREL_TYPE}})] =
  {{CONDITION}}.{{GROUP_BY_KEY}};
{{OUTPUT}} = keyed_{{CLASS_NAME}}.groupByKey().map(({{INPUT}} => { {{NEXT_OPERATOR}} })){{TO_CACHE}}

