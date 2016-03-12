public IEnumerable<{{OUTPUT_TYPE}}> {{FUN_NAME}}({{KEY_TYPE}} key, IEnumerable<{{ROW_TYPE}}> values) {
  int outputValue = 0;
  foreach (var value in values)
    outputValue++;
  yield return new {{OUTPUT_TYPE}}({{KEY_OUT}} outputValue);
}
