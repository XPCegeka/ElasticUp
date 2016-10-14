using ElasticUp.Tests.Infrastructure;

namespace ElasticUp.Tests
{
    public abstract class AbstractIntegrationTest
    {
        protected ElasticSearchContainer SetupElasticSearchService()
        {
            var esService = ElasticSearchContainer.StartNewFromArchive(Resources.elasticsearch_2_4_1_with_head_and_delete_by_query);
            esService.WaitUntilElasticOperational();

            return esService;
        }
    }
}