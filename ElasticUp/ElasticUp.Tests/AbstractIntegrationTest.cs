using System;
using ElasticUp.Tests.Infrastructure;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        private ElasticSearchContainer _esService;
        protected readonly IElasticClient ElasticClient = new ElasticClient(new Uri("http://localhost:9200/"));
        
        [SetUp]
        protected void SetUp()
        {
            _esService = StartAndWaitForElasticSearchService();
        }

        [TearDown]
        protected void TearDown()
        {
            _esService.Dispose();
        }
        
        private static ElasticSearchContainer StartAndWaitForElasticSearchService()
        {
            var esService = ElasticSearchContainer.StartNewFromArchive(Resources.elasticsearch_2_4_1_with_head_and_delete_by_query);
            esService.WaitUntilElasticOperational();

            return esService;
        }
    }
}