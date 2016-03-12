val keyed_{{CLASS_NAME}}:org.apache.spark.rdd.RDD[({{COL_TYPE}},{{REL_TYPE}})] = {{REL_NAME}}.({{KEYRDD}})
val desc_{{CLASS_NAME}}:Boolean = {{ORDER}}
{{CLASS_NAME}} = keyed_{{CLASS_NAME}}.sortByKey({{ORDER}}, 1).values

