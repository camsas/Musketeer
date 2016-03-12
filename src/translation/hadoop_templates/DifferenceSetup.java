      String relation = ((FileSplit)context.getInputSplit()).getPath()
        .getParent().getName();
      if (relation.compareTo("{{LEFT_REL}}") == 0) {
        is_left_rel = true;
      }
