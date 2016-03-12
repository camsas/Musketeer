public IEnumerable<{{OUTPUT_TYPE}}> {{FUN_NAME}}({{KEY_TYPE}} key, IEnumerable<{{ROW_TYPE}}> values) {
  {{VAL_TYPE}} outValue = {{INIT_VAL}};
  foreach (var value in values)
    {{UPDATE_VAL}};
  yield return new {{OUTPUT_TYPE}}({{KEY_OUT}} outValue);
}
