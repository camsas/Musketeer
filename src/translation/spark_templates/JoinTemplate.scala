val keyed_{{REL_NAME1}}_{{CLASS_NAME}}:org.apache.spark.rdd.RDD[({{JOIN_COL_TYPE}},{{INPUTREL_TYPE1}})] = {{REL_NAME1}}.{{KEYRDD1}};
val keyed_{{REL_NAME2}}_2{{CLASS_NAME}}:org.apache.spark.rdd.RDD[({{JOIN_COL_TYPE}},{{INPUTREL_TYPE2}})] =  {{REL_NAME2}}.{{KEYRDD2}};
val int_{{OUTPUT}} = keyed_{{REL_NAME1}}_{{CLASS_NAME}}.join(keyed_{{REL_NAME2}}_2{{CLASS_NAME}})
{{OUTPUT}} = int_{{OUTPUT}}.map(({{INPUT}} => { {{NEXT_OPERATOR}} })){{TO_CACHE}}

