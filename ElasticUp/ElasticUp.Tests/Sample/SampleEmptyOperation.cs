using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyOperation : AbstractElasticUpOperation
    {
        public override void Execute(IElasticClient elasticClient) {}
    }
}