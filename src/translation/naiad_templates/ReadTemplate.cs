public IEnumerable<{{TYPE}}> {{FUN_REL_NAME}}(string filename) {
  StreamReader reader = File.OpenText(filename);
  for (; !reader.EndOfStream; ) {
    var elements = reader.ReadLine().Split(' ');
    yield return {{GENERATE_COLS}};
  }
}