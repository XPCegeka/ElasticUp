using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Management;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace ElasticUp.Tests.Infrastructure
{
    public class ElasticSearchContainer : IDisposable
    {
        private readonly string _tempDirectory;
        private readonly Process _esProcess;

        public ElasticSearchContainer(Stream elasticSearchArchive)
        {
            _tempDirectory = GetTempDirectory();

            using (elasticSearchArchive)
            using(var elasticSearch = new ZipArchive(elasticSearchArchive))
            {
                elasticSearch.ExtractToDirectory(_tempDirectory);
            }

            _esProcess = StartElasticSearch();
        }

        public static ElasticSearchContainer StartNewFromArchive(byte[] elasticSearchArchive)
        {
            return new ElasticSearchContainer(new MemoryStream(elasticSearchArchive));
        }

        public void WaitUntilElasticOperational()
        {
            SpinWait.SpinUntil(() => IsElasticSearchUpAndRunning().Result);
        }

        public void Dispose()
        {
            KillProcessAndChildren(_esProcess.Id);
            CleanupTempDirectory();
        }

        private void CleanupTempDirectory()
        {
            SpinWait.SpinUntil(() =>
            {
                try
                {
                    Directory.Delete(_tempDirectory, true);
                    return true;
                }
                catch
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30));
        }

        private Process StartElasticSearch()
        {
            var esBinPath = Path.Combine(_tempDirectory, @"bin\elasticsearch.bat");
            var processInfo = new ProcessStartInfo("cmd.exe", $"/c {esBinPath}")
            {
                CreateNoWindow = true,
                UseShellExecute = false
            };

            return Process.Start(processInfo);
        }

        private static async Task<bool> IsElasticSearchUpAndRunning()
        {
            using (var httpClient = new HttpClient())
            using (var request = new HttpRequestMessage(HttpMethod.Get, "http://localhost:9200"))
            {
                try
                {
                    var response = await httpClient.SendAsync(request);
                    return response.IsSuccessStatusCode;
                }
                catch
                {
                    return false;
                }
            }
        }

        private static string GetTempDirectory()
        {
            var tempFile = Path.GetTempFileName();
            return Path.ChangeExtension(tempFile, null);
        }

        /// <summary>
        /// Kill a process, and all of its children, grandchildren, etc.
        /// </summary>
        /// <param name="pid">Process ID.</param>
        /// <sauce>http://stackoverflow.com/a/10402906</sauce>
        private static void KillProcessAndChildren(int pid)
        {
            var searcher = new ManagementObjectSearcher
              ("Select * From Win32_Process Where ParentProcessID=" + pid);
            var moc = searcher.Get();
            foreach (var mo in moc.Cast<ManagementObject>())
            {
                KillProcessAndChildren(Convert.ToInt32(mo["ProcessID"]));
            }
            try
            {
                var proc = Process.GetProcessById(pid);
                proc.Kill();
            }
            catch (ArgumentException)
            {
                // Process already exited.
            }
        }
    }
}
