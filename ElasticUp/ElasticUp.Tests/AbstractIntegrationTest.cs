using ElasticUp.Tests.Infrastructure;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        private readonly ElasticServiceStartup _elasticServiceStartup;
        private ElasticSearchContainer _esService;

        protected AbstractIntegrationTest(
            ElasticServiceStartup elasticServiceStartup = ElasticServiceStartup.NoStartup)
        {
            _elasticServiceStartup = elasticServiceStartup;
        }

        [SetUp]
        protected void SetUp()
        {
            if (_elasticServiceStartup == ElasticServiceStartup.StartupForEach)
                _esService = StartAndWaitForElasticSearchService();
        }

        [TearDown]
        protected void TearDown()
        {
            if (_elasticServiceStartup == ElasticServiceStartup.StartupForEach)
                _esService.Dispose();
        }

        [OneTimeSetUp]
        protected void OneTimeSetUp()
        {
            if (_elasticServiceStartup == ElasticServiceStartup.OneTimeStartup)
                _esService = StartAndWaitForElasticSearchService();
        }

        [OneTimeTearDown]
        protected void OneTimeTearDown()
        {
            if (_elasticServiceStartup == ElasticServiceStartup.OneTimeStartup)
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