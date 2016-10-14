using System;
using ElasticUp.Tests.Infrastructure;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        public IElasticClient ElasticClient;

        [OneTimeSetUp]
        public void AbstractSetup()
        {
            CreateElasticClient();
        }

        protected ElasticSearchContainer SetupElasticSearchService()
        {
            var esService = ElasticSearchContainer.StartNewFromArchive(Resources.elasticsearch_2_4_1_with_head_and_delete_by_query);
            esService.WaitUntilElasticOperational();

            return esService;
        }

        protected void CreateElasticClient()
        {
            ElasticClient = new ElasticClient(new Uri("http://localhost:9200/"));
        }
    }
}