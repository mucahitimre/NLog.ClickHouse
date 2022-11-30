using NLog;
using NLog.Fluent;
using System;
using System.IO;

namespace NLog.ClickHouse.Example
{
    internal class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static void Main(string[] args)
        {
            int k = 42;
            int l = 100;

            for (int i = 0; i < 1000; i++)
            {
                Logger.Trace("Sample trace message, k={0}, l={1}", k, l);
                Logger.Debug("Sample debug message, k={0}, l={1}", k, l);
                Logger.Info("Sample informational message, k={0}, l={1}", k, l);
                Logger.Warn("Sample warning message, k={0}, l={1}", k, l);
                Logger.Error("Sample error message, k={0}, l={1}", k, l);
                Logger.Fatal("Sample fatal error message, k={0}, l={1}", k, l);
                Logger.Log(LogLevel.Info, "Sample fatal error message, k={0}, l={1}", k, l);

                Logger.Info().Message("Sample informational message, k={0}, l={1}", k, l);

                string path = "blah.txt";
                try
                {
                    string text = File.ReadAllText(path);
                }
                catch (Exception ex)
                {
                    Logger.Fatal(ex);
                }
            }

            Console.WriteLine("Ok..");
            Console.ReadLine();
        }
    }
}
