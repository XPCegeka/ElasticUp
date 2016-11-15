using Nest;

namespace ElasticUp.Migration
{
    public abstract class CustomElasticUpMigration : AbstractElasticUpMigration
    {
        protected CustomElasticUpMigration(IElasticClient elasticClient) : base(elasticClient) { }
    }
}
