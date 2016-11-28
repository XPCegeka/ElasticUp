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
        protected string MigrationHistoryTestIndexName;

        protected IElasticClient ElasticClient;
        
        [SetUp]
        protected void SetUp()
        {
            MigrationHistoryTestIndexName = TestContext.CurrentContext.Test.MethodName.ToLowerInvariant() + "-" + "migrationhistory";
            TestIndex = new VersionedIndexName(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant(), 0);

            var settings = new ConnectionSettings(new Uri(ElasticSearchUrl))
                .DefaultIndex(TestIndex.IndexNameWithVersion())
                .ThrowExceptions(true);

            ElasticClient = new ElasticClient(settings);

            ElasticClient.CreateIndex(
                MigrationHistoryTestIndexName,
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));

            ElasticClient.CreateIndex(
                TestIndex.IndexNameWithVersion(), 
                indexDescriptor => indexDescriptor.Settings(indexSettings => indexSettings
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)));

            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
        }

        [TearDown]
        protected void TearDown()
        {
            ElasticClient.DeleteIndex(TestIndex.IndexNameWithVersion());
        }
      
    }
}