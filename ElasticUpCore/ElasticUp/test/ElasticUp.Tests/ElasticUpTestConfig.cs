using System.IO;
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
            var zip = File.ReadAllBytes("Resources/elasticsearch-2.4.1-with-head-and-delete_by_query.zip");
            var elasticSearchContainer = ElasticSearchContainer.StartNewFromArchive(zip);
            elasticSearchContainer.WaitUntilElasticOperational();
            return elasticSearchContainer;
        }
    }
    
}
