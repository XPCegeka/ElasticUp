using System;
using ElasticUp.Migration.Meta;
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
                .ThrowExceptions(true);

            ElasticClient = new ElasticClient(settings);

            CreateMigrationHistoryTestIndex();
            CreateTestIndex();
            CreateNextTestIndex();
        }

        private void CreateMigrationHistoryTestIndex()
        {
            ElasticClient.CreateIndex(
                MigrationHistoryTestIndex.IndexNameWithVersion(),
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));
            ElasticClient.PutAlias(MigrationHistoryTestIndex.IndexNameWithVersion(), MigrationHistoryTestIndex.AliasName);
        }

        protected void CreateTestIndex()
        {
            ElasticClient.CreateIndex(
                TestIndex.IndexNameWithVersion(),
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));

            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
        }

        protected void CreateNextTestIndex()
        {
            ElasticClient.CreateIndex(
                TestIndex.NextIndexNameWithVersion(),
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));
        }

        [TearDown]
        protected void TearDown()
        {
            TryDeleteIndex(TestIndex.IndexNameWithVersion());
        }


        protected void TryDeleteIndex(string indexName)
        {
            try
            {
                ElasticClient.DeleteIndex(indexName);
            }
            catch(Exception) { }
        } 
    }
}