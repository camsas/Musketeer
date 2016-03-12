      if ({{CONDITION}}) {
        {{COL_TYPE}} col = {{COL_TYPE}}.valueOf({{REL_NAME}}[{{COL_INDEX}}]);
        context.write(new {{HADOOP_COL_TYPE}}(col), value);
      }
