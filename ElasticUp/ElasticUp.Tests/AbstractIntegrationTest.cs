using System;
using ElasticUp.Tests.Infrastructure;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        private readonly ElasticServiceStartupType _elasticServiceStartupType;
        private ElasticSearchContainer _esService;
        protected readonly IElasticClient ElasticClient = new ElasticClient(new Uri("http://localhost:9200/"));

        protected AbstractIntegrationTest(ElasticServiceStartupType elasticServiceStartupType = ElasticServiceStartupType.NoStartup)
        {
            _elasticServiceStartupType = elasticServiceStartupType;
        }

        [SetUp]
        protected void SetUp()
        {
            if (_elasticServiceStartupType == ElasticServiceStartupType.StartupForEach)
                _esService = StartAndWaitForElasticSearchService();
        }

        [TearDown]
        protected void TearDown()
        {
            if (_elasticServiceStartupType == ElasticServiceStartupType.StartupForEach)
                _esService.Dispose();
        }

        [OneTimeSetUp]
        protected void OneTimeSetUp()
        {
            if (_elasticServiceStartupType == ElasticServiceStartupType.OneTimeStartup)
                _esService = StartAndWaitForElasticSearchService();
        }

        [OneTimeTearDown]
        protected void OneTimeTearDown()
        {
            if (_elasticServiceStartupType == ElasticServiceStartupType.OneTimeStartup)
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