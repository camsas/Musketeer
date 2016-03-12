using Naiad;
using Naiad.Dataflow;
using Naiad.Frameworks.DifferentialDataflow;
using Naiad.Runtime.Controlling;
using Naiad.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Examples.DifferentialDataflow {

  public class {{CLASS_NAME}}: Example {

    public void Execute(string[] args) {
      using (var controller = NewController.FromArgs(ref args)) {
        using (var manager = controller.NewGraph()) {
          manager.Activate();
        }
        controller.Join();
      }
    }

    public string Usage { get { return ""; } }

  }
}