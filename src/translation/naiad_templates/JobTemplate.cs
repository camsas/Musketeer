using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Input;

namespace Musketeer {

  public class {{CLASS_NAME}} : Example {

{{STRUCTS_CODE}}

    {{READ_CODE}}

    {{FUN_CODE}}

    {{COMPACTOR_CODE}}

    public string Usage {get {return "";} }

    public void Execute(string[] args) {
      using (var computation = NewComputation.FromArgs(ref args)) {
        {{VARS_DECLARATION}}
        {{INPUT_VARS}}
        {{CODE}}
        int minThreadId = computation.Configuration.ProcessID *
          computation.Configuration.WorkerCount;
        {{OUTPUT_CODE}}
        computation.Activate();
        if (computation.Configuration.ProcessID == 0) {
          {{ON_COMPLETED_MASTER}}
        } else {
          {{ON_COMPLETED_WORKER}}
        }
        computation.Join();
        {{OUTPUT_CLOSE_CODE}}
      }
    }

  }

}