public struct {{REDUCER_NAME}} : IAddable<{{REDUCER_NAME}}>
{

  {{REDUCER_VARS}}

  public {{REDUCER_NAME}}({{REDUCER_ARGS}})
  {
    {{REDUCER_CONSTRUCTOR}}
  }

  public {{REDUCER_NAME}} Add({{REDUCER_NAME}} other)
  {
    {{UPDATE_VARS}}
    return this;
  }

}