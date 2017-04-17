using System;
using System.Diagnostics;
using System.Linq;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Infrastructure;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using Newtonsoft.Json;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    [Ignore("experiments")]
    public class ReindexTypeOperationPerformanceIntegrationTest : AbstractIntegrationTest
    {
        private const int NumberOfObjects = 500000;
        private const int ChunkSize = 10000;

        private dynamic _largeObject;

        [SetUp]
        public void Setup()
        {
            var largeJson = ResourceUtilities.FromResourceFileToString("large_document.json");
            _largeObject = JsonConvert.DeserializeObject<dynamic>(largeJson);

            Enumerable.Range(0, NumberOfObjects / ChunkSize)
                .Select(startId => startId * ChunkSize)
                .AsParallel()
                .WithDegreeOfParallelism(10)
                .ForAll(BulkIndex);

            ElasticClient.Refresh(Indices.All);
        }

        private void BulkIndex(int startId)
        {
            var bulkDescriptor = new BulkDescriptor();

            foreach (var id in Enumerable.Range(startId, ChunkSize))
            {
                bulkDescriptor.Index<object>(
                    descr => descr.Index(TestIndex.IndexNameWithVersion())
                        .Id($"{id}")
                        .Type("largetype")
                        .Document(_largeObject as object));
            }

            ElasticClient.Bulk(bulkDescriptor);
        }

        private void Reindex(string fromIndex, string toIndex)
        {
            var timer = new Stopwatch();
            timer.Start();
            new ReindexTypeOperation("largetype")
                    .FromIndex(fromIndex)
                    .ToIndex(toIndex)
                    .Execute(ElasticClient);
            timer.Stop();
            Console.WriteLine($@" took {TimeSpan.FromMilliseconds(timer.ElapsedMilliseconds).TotalSeconds} seconds");
        }

        [Test]
        public void TomsRefreshIntervalExperimentWithReindex_LargeData()
        {
            var fromIndex = TestIndex;
            var toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());

            //REINDEX 1 (without refreshinterval)
            Console.Write(@"Without refresh interval to -1: ");
            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);
            

            //REINDEX 2 (with refreshinterval)
            fromIndex = toIndex;
            toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());
            CreateIndex(toIndex.IndexNameWithVersion());
            
            Console.Write(@"With refresh interval to -1");
            ElasticClient.UpdateIndexSettings(toIndex.IndexNameWithVersion(), s => s.IndexSettings(p => p.RefreshInterval(new Time(-1))));

            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);


            //REINDEX 3 (with refreshinterval)
            fromIndex = toIndex;
            toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());
            CreateIndex(toIndex.IndexNameWithVersion());

            Console.Write(@"Without refresh interval to -1: ");
            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);


            //REINDEX 4 (with refreshinterval)
            fromIndex = toIndex;
            toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());
            CreateIndex(toIndex.IndexNameWithVersion());

            Console.Write(@"With refresh interval to -1");
            ElasticClient.UpdateIndexSettings(toIndex.IndexNameWithVersion(), s => s.IndexSettings(p => p.RefreshInterval(new Time(-1))));

            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);


            //REINDEX 5 (with refreshinterval)
            fromIndex = toIndex;
            toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());
            CreateIndex(toIndex.IndexNameWithVersion());

            Console.Write(@"Without refresh interval to -1: ");
            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);


            //REINDEX 6 (with refreshinterval)
            fromIndex = toIndex;
            toIndex = VersionedIndexName.CreateFromIndexName(fromIndex.NextIndexNameWithVersion());
            CreateIndex(toIndex.IndexNameWithVersion());

            Console.Write(@"With refresh interval to -1");
            ElasticClient.UpdateIndexSettings(toIndex.IndexNameWithVersion(), s => s.IndexSettings(p => p.RefreshInterval(new Time(-1))));

            Reindex(fromIndex, toIndex);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<object>(descriptor => descriptor.Type("largetype").Index(toIndex.IndexNameWithVersion())).Count.Should().Be(NumberOfObjects);

        }
    }
    
}