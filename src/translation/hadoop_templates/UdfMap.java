      {{UDF_CLASS}} udf = new {{UDF_CLASS}}();
      context.write(NullWritable.get(),
                    new Text(udf.evaluate(value.toString())));
