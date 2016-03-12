      String {{OUTPUT_REL}}_output = "";
      for (int output_it = 0; output_it < {{OUTPUT_REL}}.length; output_it++) {
        {{OUTPUT_REL}}_output += {{OUTPUT_REL}}[output_it] + " ";
      }
      context.write(NullWritable.get(), new Text({{OUTPUT_REL}}_output.trim()));