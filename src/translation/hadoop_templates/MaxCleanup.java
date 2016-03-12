      if ({{GROUP_BY}} == -1 && maxValue != null) {
        context.write(new Text("ALL"),
                      new Text(String.valueOf(maxValue) + selectedCols));
      }
