using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Migration.Meta;
using ElasticUp.Tests.Sample;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        private const string ElasticSearchUrl = "http://localhost:9201";
        protected VersionedIndexName TestIndex;
        protected VersionedIndexName MigrationHistoryTestIndex;

        protected IElasticClient ElasticClient;
        
        [SetUp]
        protected void SetUp()
        {
            MigrationHistoryTestIndex = new VersionedIndexName(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant() + "-" + "migrationhistory", 0);
            TestIndex = new VersionedIndexName(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant(), 0);

            var settings = new ConnectionSettings(new Uri(ElasticSearchUrl))
                .DefaultIndex(TestIndex.IndexNameWithVersion())
                .DisableDirectStreaming(true)
                .ThrowExceptions(true)
                .RequestTimeout(TimeSpan.FromHours(8));

            ElasticClient = new ElasticClient(settings);

            CreateMigrationHistoryTestIndex();
            CreateTestIndex();
            CreateNextTestIndex();
        }

        private void CreateMigrationHistoryTestIndex()
        {
            CreateIndex(MigrationHistoryTestIndex.IndexNameWithVersion());
            ElasticClient.PutAlias(MigrationHistoryTestIndex.IndexNameWithVersion(), MigrationHistoryTestIndex.AliasName);
        }

        protected void CreateTestIndex()
        {
            CreateIndex(TestIndex.IndexNameWithVersion());
            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
        }

        protected void CreateNextTestIndex()
        {
            CreateIndex(TestIndex.NextIndexNameWithVersion());
        }

        protected void CreateIndex(string index)
        {
            ElasticClient.CreateIndex(
                index,
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));
        }

        [TearDown]
        protected void TearDown()
        {
            TryDeleteIndex(TestIndex.IndexNameWithVersion());
            TryDeleteIndex(TestIndex.NextIndexNameWithVersion());
            TryDeleteIndex(MigrationHistoryTestIndex);
        }


        protected void TryDeleteIndex(string indexName)
        {
            try
            {
                ElasticClient.DeleteIndex(indexName);
            }
            catch(Exception) { }
        }

        protected void BulkIndexSampleObjects(int numberOfObjects, int chunkSize = 5000)
        {

            SplitList(Enumerable.Range(0, numberOfObjects).ToList(), chunkSize)
                .AsParallel()
                .WithDegreeOfParallelism(10)
                .ForAll(BulkIndexFrom);

            ElasticClient.Refresh(Indices.All);
        }

        private void BulkIndexFrom(List<int> numbers)
        {
            var bulkDescriptor = new BulkDescriptor();

            foreach (var number in numbers)
            {
                bulkDescriptor.Index<SampleObject>(
                    descr => descr.Index(TestIndex.IndexNameWithVersion())
                        .Id($"{number}")
                        .Document(new SampleObject { Number = number }));
            }

            ElasticClient.Bulk(bulkDescriptor);
        }

        private static List<List<int>> SplitList(List<int> locations, int nSize=30)
        {
            var output = new List<List<int>>();

            for (var i=0; i < locations.Count; i+= nSize)
            {
                output.Add(locations.GetRange(i, Math.Min(nSize, locations.Count - i)));
            }

            return output;
        }

    }
}