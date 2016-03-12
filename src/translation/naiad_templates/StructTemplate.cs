  public struct {{TYPE}} : IEquatable<{{TYPE}}> {
{{FIELDS}}
    public bool Equals({{TYPE}} that) {
      return {{#EQUALS}}{{EQUAL_TEST}}{{#EQUALS_separator}} && {{/EQUALS_separator}}{{/EQUALS}};
    }

    public int CompareTo({{TYPE}} that) {
      {{#COMPARE_TO}}{{COMPARE_TEST}}{{#COMPARE_TO_separator}}
      {{/COMPARE_TO_separator}}{{/COMPARE_TO}}
      return 0;
    }

    // Embarassing hashcodes
    public override int GetHashCode() {
      return {{HASH_CODE}};
    }

    public override string ToString() {
      return String.Format("{{#TO_STRING_FORMAT}}{{TO_STRING_FORMAT_ENTRY}}{{#TO_STRING_FORMAT_separator}} {{/TO_STRING_FORMAT_separator}}{{/TO_STRING_FORMAT}}" {{TO_STRING_ARGS}});
    }

    public {{TYPE}}({{#CONSTRUCTOR_ARGS}}{{ARG_TYPE}} {{ARG_NAME}}{{#CONSTRUCTOR_ARGS_separator}}, {{/CONSTRUCTOR_ARGS_separator}}{{/CONSTRUCTOR_ARGS}}) {
{{CONSTRUCTOR}}
    }

  }
