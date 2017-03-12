using System;
using System.Diagnostics;

namespace ElasticUp.Util
{
    public class ElasticUpTimer : IDisposable
    {
        private readonly Stopwatch _stopwatch;
        private readonly string _name;

        public ElasticUpTimer(string name = "")
        {
            _name = name;
            _stopwatch = Stopwatch.StartNew();
        }

        public long StopAndGetElapsedMilliseconds()
        {
            Stop();
            return GetElapsedMilliseconds();
        }

        public long GetElapsedMilliseconds()
        {
            return _stopwatch.ElapsedMilliseconds;
        }

        private void Stop()
        {
            if (_stopwatch.IsRunning)
            {
                _stopwatch.Stop();
            }
        }

        private void Log()
        {
            Console.WriteLine($@"Timer {_name} ran for {TimeSpan.FromMilliseconds(_stopwatch.ElapsedMilliseconds).TotalSeconds} seconds");
        }

        public void Dispose()
        {
            Stop();
            Log();
        }
    }
}