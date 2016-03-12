  var {{REL_NAME}}_st = sc.textFile("{{HDFS_MASTER}}{{INPUT_PATH}}")
  var {{REL_NAME}} = {{REL_NAME}}_st.map((line:String) => {
    var splitted:Array[String] = line.split(" ");
    {{ARGS}}
  }){{TO_CACHE}}
