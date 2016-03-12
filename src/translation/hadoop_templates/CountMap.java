      if ({{CONDITION}}) {
        if ({{GROUP_BY}} == -1) {
          cnt += 1;
        } else {
          Text newKey = new Text({{GROUP_BY_KEY}});
          context.write(newKey, new Text("1"));
        }
      }
