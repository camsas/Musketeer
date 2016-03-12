      if ({{GROUP_BY}} == -1 && aggValues[0] != null) {
        String outputValue = String.valueOf(aggValues[0]);
        for (int i = 1; i < aggValues.length; i++) {
          outputValue += " " + String.valueOf(aggValues[i]);
        }
        context.write(new Text("ALL"), new Text(outputValue));
      }
