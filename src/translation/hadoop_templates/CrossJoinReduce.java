      List<String> arrayLeft = new LinkedList<String>();
      List<String> arrayRight = new LinkedList<String>();
      for (Text text : values) {
        String tmp = text.toString();
        if (tmp.charAt(0) == 'L') {
          arrayLeft.add(tmp.substring(2));
        } else {
          arrayRight.add(tmp.substring(2));
        }
      }
      for (String left : arrayLeft) {
        for (String right : arrayRight) {
          String[] {{OUTPUT_REL}} = (left + " " + right).split(" ");
          {{NEXT_OPERATOR}}
          {{OUTPUT_CODE}}
        }
      }
