using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gentrack.Tools.DataReplicationLoadTool.Consumers;
using Gentrack.Tools.DataReplicationLoadTool.Producers;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.DependencyInjection;
using CommandLine;


namespace Gentrack.Tools.DataReplicationLoadTool
{
    class Program
    {
        private class CommandLineOptions
        {
            //commit options here
            [Option('m', "mode", Required = true, HelpText = "Set processing mode to ether \"Full\" or \"Delta\"")]
            public string Type { get; set; }
        }

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<CommandLineOptions>(args)
                .WithParsed<CommandLineOptions>(opts => RunOptionsAndReturnExitCode(opts))
                .WithNotParsed<CommandLineOptions>((errs) => HandleParseError(errs.ToList()));
        }

        private static void HandleParseError(List<Error> errorList)
        {
            foreach (var error in errorList)
            {
                Console.WriteLine(error.Tag);
            }
            
        }

        private static void WaitForUserQuit(CancellationToken cancelToken)
        {
            while (!Console.ReadLine().Equals("Q") && !cancelToken.IsCancellationRequested)
            {
                //Wait for User to Quit
            }
        }
        private static void RunOptionsAndReturnExitCode(CommandLineOptions opts)
        {

            Console.WriteLine("Starting Gentrack.Tools.DataReplicationLoadTool");

            ConcurrentQueue<FileObject> masterQueue = new ConcurrentQueue<FileObject>();

            var cancelTokenSource = new CancellationTokenSource();
            var cancelToken = cancelTokenSource.Token;

            IServiceCollection services = new ServiceCollection();

            Startup startup = new Startup();
            startup.ConfigureServices(services);

            IServiceProvider serviceProvider = services.BuildServiceProvider();

            IFileProducer fileProducer = serviceProvider.GetService<IFileProducer>();
            Task fileProducerTask = fileProducer.StartPolling(masterQueue, cancelToken);

            Task fileConsumerTask;

            if (opts.Type.Equals("Full"))
            {
                IFullLoadFileConsumer fileConsumer = serviceProvider.GetService<IFullLoadFileConsumer>();
                fileConsumerTask = fileConsumer.StartPolling(masterQueue, cancelToken);
            }
            else if (opts.Type.Equals("Delta"))
            {
                IDeltaFileConsumer fileConsumer = serviceProvider.GetService<IDeltaFileConsumer>();
                fileConsumerTask = fileConsumer.StartPolling(masterQueue, cancelToken);
            }
            else
            {
                throw new System.ArgumentException("Run mode must be selected");
            }

            Task userInputTask = Task.Run(() => WaitForUserQuit(cancelToken));

            Task.WaitAny(new Task[] { fileProducerTask, fileConsumerTask, userInputTask });

            cancelTokenSource.Cancel();

            Console.WriteLine("Trying to shutdown");

            try
            {
                Task.WaitAll(new Task[] { fileProducerTask, fileConsumerTask });
            }
            catch (AggregateException ae)
            {
                foreach (var e in ae.InnerExceptions)
                {
                  Console.WriteLine(e.ToString());  
                }
                throw;
            }

            Console.WriteLine("END0");


        }
    }
}
