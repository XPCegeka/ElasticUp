using System.Collections.Generic;
using Nest;

namespace ElasticUp.Migration.Meta
{
    public class AliasHelper
    {
        private readonly IElasticClient _elasticClient;

        public AliasHelper(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public virtual IEnumerable<string> GetIndexNamesForAlias(string alias)
        {
            return _elasticClient.GetIndicesPointingToAlias(alias);
        }

        public virtual void RemoveAliasOnIndices(string alias, params string[] indexNames)
        {
            _elasticClient.DeleteAlias(Indices.Parse(string.Join(",", indexNames)), alias);
        }

        public virtual void AddAliasOnIndices(string alias, params string[] indexNames)
        {
            _elasticClient.PutAlias(Indices.Parse(string.Join(",", indexNames)), alias);
        }
    }
}