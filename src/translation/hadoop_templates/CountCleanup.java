      if ({{GROUP_BY}} == -1) {
        context.write(new Text("ALL"), new Text(String.valueOf(cnt)));
      }
