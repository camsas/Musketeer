      if (this.is_left_rel) {
        context.write(new Text("1"),
                      new Text("L " + value.toString()));
      } else {
        context.write(new Text("1"),
                      new Text("R " + value.toString()));
      }
