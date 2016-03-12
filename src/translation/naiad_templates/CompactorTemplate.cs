internal class RowsCompactor<T> : Naiad.Frameworks.UnaryVertex<{{LEFT_ROW_TYPE}}, CompactRows, T>
where T : Time<T>
{

  private readonly Dictionary<T, CompactBuilder> Rows =
    new Dictionary<T, CompactBuilder>();

  public override void MessageReceived(Message<Pair<{{LEFT_ROW_TYPE}}, T>> message)
  {
    T time = message.payload[0].v2;
    if (!Rows.ContainsKey(time))
    {
      Rows.Add(time, new CompactBuilder());
      NotifyAt(time);
    }
    var builder = Rows[time];
    for (int i = 0;  i < message.length; i++)
    {
      builder.AddRow(message.payload[i].v1);
    }
    Rows[time] = builder;
  }

  public override void OnDone(T time)
  {
    if (Rows.ContainsKey(time))
    {
      Output.Send(Rows[time].Build(Stage.Placement.Count), time);
      Rows.Remove(time);
    }
  }

  public RowsCompactor(int index, Stage<T> vertex) : base(index, vertex)
  {
  }

}

internal class JoinOpt<T> : Naiad.Frameworks.BinaryVertex<CompactRows, {{RIGHT_ROW_TYPE}}, {{OUTPUT_TYPE}}, T>
        where T : Time<T>
{

  CompactRows Rows;
  SpinedList<{{RIGHT_ROW_TYPE}}> ToProcess;

  public override void MessageReceived1(Message<Pair<CompactRows, T>> message)
  {
    for (int i = 0; i < message.length; i++)
    {
      Rows = message.payload[i].v1;
    }
    NotifyAt(message.payload[0].v2);
  }

  public override void MessageReceived2(Message<Pair<{{RIGHT_ROW_TYPE}}, T>> message)
  {
    if (Rows.keys == null && message.length > 0)
    {
      for (int i = 0; i < message.length; i++)
      {
        ToProcess.Add(message.payload[i].v1);
      }
    }
    else
    {
      for (int i = 0; i < message.length; i++)
      {
        var record = message.payload[i].v1;
        var localName = record.{{RIGHT_KEY_INDEX}} / Stage.Placement.Count;
        var time =  message.payload[i].v2;
        if (localName + 1 < Rows.keys.Length)
        {
          for (int j = Rows.keys[localName]; j < Rows.keys[localName + 1]; j++)
          {
            var remainderRow = Rows.remainderRows[j];
            Output.Send({{UPDATE_REMAINDER}}, time);
          }
        }
      }
    }
  }

  public override void OnDone(T time)
  {
    foreach (var record in ToProcess.AsEnumerable())
    {
      var localName = record.{{RIGHT_KEY_INDEX}} / Stage.Placement.Count;
      if (localName + 1 < Rows.keys.Length)
      {
        for (int j = Rows.keys[localName]; j < Rows.keys[localName + 1]; j++)
        {
          var remainderRow = Rows.remainderRows[j];
          Output.Send({{UPDATE_REMAINDER}}, time);
        }
      }
    }
    ToProcess = new SpinedList<{{RIGHT_ROW_TYPE}}>();
  }

  public JoinOpt(int index, Stage<T> vertex) : base(index, vertex)
  {
    Rows = new CompactRows();
    ToProcess = new SpinedList<{{RIGHT_ROW_TYPE}}>();
    Entrancy = 5;
  }
}


internal struct CompactRows
{

  public readonly Int32[] keys;
  public readonly {{REMAINDER_TYPE}}[] remainderRows;

  public CompactRows(Int32[] keys, {{REMAINDER_TYPE}}[] remainderRows)
  {
    this.keys = keys;
    this.remainderRows = remainderRows;
  }

}


internal struct CompactBuilder
{

  private SpinedList<{{LEFT_ROW_TYPE}}> newRows;

  public CompactBuilder AddRow({{LEFT_ROW_TYPE}} row)
  {
    if (newRows == null)
    {
      newRows = new SpinedList<{{LEFT_ROW_TYPE}}>();
    }
    newRows.Add(row);
    return this;
  }

  public CompactRows Build(int parts)
  {
    if (newRows == null)
    {
      newRows = new SpinedList<{{LEFT_ROW_TYPE}}>();
    }

    //TODO(ionel): select the key
    var maxKey = 0L;
    for (int i = 0; i < newRows.Count; i++)
    {
      if (maxKey < newRows[i].{{LEFT_KEY_INDEX}} / parts)
      {
        maxKey = newRows[i].{{LEFT_KEY_INDEX}} / parts;
      }
    }

    var keys = new Int32[maxKey + 2];
    var remainderRows = new {{REMAINDER_TYPE}}[newRows.Count];

    for (int i = 0; i < newRows.Count; i++)
    {
      keys[newRows[i].{{LEFT_KEY_INDEX}} / parts]++;
    }

    for (int i = 1; i < keys.Length; keys[i] += keys[i - 1], i++);

    keys[0] = 0;

    for (int i = 0; i < newRows.Count; i++)
    {
      remainderRows[keys[newRows[i].{{LEFT_KEY_INDEX}} / parts]++] =
        {{BUILD_REMAINDER}};
    }

    for (int i = keys.Length - 1; i > 0; keys[i] = keys[i - 1], i--);
    keys[0] = 0;

    return new CompactRows(keys, remainderRows);
  }

}