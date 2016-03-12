StreamWriter[] file_{{OUTPUT_REL}} = new StreamWriter[computation.Configuration.WorkerCount];
for (int i = 0; i < computation.Configuration.WorkerCount; ++i) {
  int j = minThreadId + i;
  file_{{OUTPUT_REL}}[i] = new StreamWriter("{{TMP_ROOT}}/{{OUTPUT_REL}}" + j + ".out");
}
{{OUTPUT_REL}}.Subscribe((i, l) => { foreach (var element in l) file_{{OUTPUT_REL}}[i - minThreadId].WriteLine(element); });
