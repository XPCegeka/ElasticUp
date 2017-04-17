using System.Collections.Generic;
using Nest;

namespace ElasticUp.Alias
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

        public virtual void RemoveAliasFromIndex(string alias, string indexName)
        {
            _elasticClient.Alias(descriptor => descriptor.Remove(removeDescriptor => removeDescriptor.Alias(alias).Index(indexName)));
        }

        public virtual void PutAliasOnIndex(string alias, string indexName)
        {
            _elasticClient.PutAlias(indexName, alias);
        }

        public virtual void SwitchAlias(string alias, string fromIndexName, string toIndexName)
        {
            _elasticClient.Alias(descriptor => 
                descriptor
                    .Add(addDescriptor => addDescriptor.Alias(alias).Index(toIndexName))
                    .Remove(removeDescriptor => removeDescriptor.Alias(alias).Index(fromIndexName)));
        }
    }
}