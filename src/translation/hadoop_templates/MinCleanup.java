      if ({{GROUP_BY}} == -1 && minValue != null) {
        context.write(new Text("ALL"),
                      new Text(String.valueOf(minValue) + selectedCols));
      }
