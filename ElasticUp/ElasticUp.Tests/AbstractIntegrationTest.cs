using ElasticUp.Tests.Infrastructure;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        protected ElasticSearchContainer SetupElasticSearchService()
        {
            var esService = ElasticSearchContainer.StartNewFromArchive(Resources.elasticsearch_2_4_1);
            esService.WaitUntilElasticOperational();

            return esService;
        }
    }
}