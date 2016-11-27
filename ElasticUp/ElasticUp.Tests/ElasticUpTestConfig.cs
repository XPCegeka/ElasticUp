using ElasticUp.Tests.Infrastructure;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    [SetUpFixture]
    public class ElasticUpTestConfig
    {
        private ElasticSearchContainer _elasticSearchContainer;

        [OneTimeSetUp]
        public void SetupElasticSearchInstance()
        {
            _elasticSearchContainer = StartAndWaitForElasticSearchService();
        }

        [OneTimeTearDown]
        public void TeardownElasticSearchInstance()
        {
            _elasticSearchContainer.Dispose();
        }
        
        private static ElasticSearchContainer StartAndWaitForElasticSearchService()
        {
            var elasticSearchContainer = ElasticSearchContainer.StartNewFromArchive(Resources.elasticsearch_2_4_1_with_head_and_delete_by_query);
            elasticSearchContainer.WaitUntilElasticOperational();
            return elasticSearchContainer;
        }
    }
    
}
