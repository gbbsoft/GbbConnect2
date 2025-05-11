using GbbLibSmall;

namespace GbbConnect2Console
{
    internal class Program
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="DontWaitForKey">don't wait for key, but just wait forever</param>
        static void Main(bool DontWaitForKey)
        {
            GbbEngine2.Configuration.Parameters.APP_ENVIRONMENT = "Console";

            Console.WriteLine();
            Console.WriteLine("GbbConnect2Console by gbbsoft");
            Console.WriteLine();

            Console.WriteLine($"Version:         : {GbbEngine2.Configuration.Parameters.APP_VERSION}");
            Console.WriteLine($"Parameters file  : {GbbEngine2.Configuration.Parameters.Parameters_GetFileName()}");
            Console.WriteLine($"Log directory    : {GbbEngine2.Configuration.Parameters.OurGetUserBaseDirectory()}");
            Console.WriteLine();

            var FileName = GbbEngine2.Configuration.Parameters.Parameters_GetFileName();

            // load parameters
            if (!File.Exists(FileName))
            {
                Console.WriteLine("ERROR: No parameters.xml file!");
                DontWaitForKey = false;
            }
            else
            {
                var Parameters = GbbEngine2.Configuration.Parameters.Load(FileName);

                // start server
                var JobManeger = new GbbEngine2.Server.JobManager();
                var Log = new Log();
                JobManeger.OurStartJobs(Parameters, Log);
            }

            if (DontWaitForKey)
            {
                Console.WriteLine("Running in service mode. Press Ctrl+C to exit.");
                // Keep the application alive until it's stopped by systemd (Ctrl+C / SIGTERM)
                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (sender, e) => {
                    Console.WriteLine("Service stopping...");
                    e.Cancel = true; // Prevent the process from terminating.
                    cts.Cancel();
                };
                try
                {
                    Task.Delay(Timeout.Infinite, cts.Token);
                }
                catch (TaskCanceledException)
                {
                    // Expected when stopping
                }
            }
            else
            {
                // Finish
                Console.WriteLine();
                Console.WriteLine("Press Enter to finish application...");
                Console.ReadLine();
                Console.WriteLine("Goodbye!");
            }
        }

        private class Log : GbbLibSmall.IOurLog
        {
            private object LogSync = new();

            public void OurLog(LogLevel LogLevel, string message, params object?[] args)
            {
                var nw = DateTime.Now;

                if (args.Length > 0)
                    message = string.Format(message, args);

                // add time
                string msg;
                if (LogLevel == LogLevel.Error)
                    msg = $"{nw}: ERROR: {message}\r\n";
                else
                    msg = $"{nw}: {message}\r\n";

                lock (LogSync)
                {

                    // directory for log
                    string FileName = Path.Combine(GbbEngine2.Configuration.Parameters.OurGetUserBaseDirectory(), "Log");
                    Directory.CreateDirectory(FileName);

                    // filename of log
                    FileName = Path.Combine(FileName, $"{nw:yyyy-MM-dd}.txt");
                    File.AppendAllText(FileName, msg);

                    // to console:
                    Console.Write(msg);
                }
            }
        }

    }
}
