using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Diagnostics;

namespace Musketeer {

  public interface Example {
    string Usage { get; }
    void Execute(string[] args);
  }

  class Program {

    static void Main(string[] args) {
      // map from example names to code to run in each case
      var examples = new Dictionary<string, Example>();

      // loading up as many examples as we can think of
      examples.Add("{{CLASS_NAME}}", new Musketeer.{{CLASS_NAME}}());

      // determine which exmample was asked for
      if (args.Length == 0 || !examples.ContainsKey(args[0].ToLower())) {
        Console.Error.WriteLine("First argument not found in list of examples");
        Console.Error.WriteLine("Choose from the following exciting options:");
        foreach (var pair in examples.OrderBy(x => x.Key))
          Console.Error.WriteLine("\tExamples.exe {0} {1} [naiad options]", pair.Key, pair.Value.Usage);
          Console.Error.WriteLine();
          Configuration.Usage();
      } else {
        var example = args[0].ToLower();
        if (args.Contains("--help") || args.Contains("/?") || args.Contains("--usage")) {
          Console.Error.WriteLine("Usage: Musketeer.exe {0} {1} [naiad options]", example,
                                  examples[example].Usage);
          Configuration.Usage();
        } else {
          Logging.LogLevel = LoggingLevel.Off;
          examples[example].Execute(args);
        }
      }
    }
  }
}
