val {{REL_NAME}}_string = {{REL_NAME}}.map({{STRING_MAPPING}})
{{REL_NAME}}_string.saveAsTextFile("{{HDFS_MASTER}}{{OUTPUT_PATH}}")