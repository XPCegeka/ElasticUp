using System.Collections.Generic;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Helper
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
        
        public virtual bool AliasExistsOnIndex(string alias, string index)
        {
            return _elasticClient.AliasExists(r => r.Index(index).Name(alias)).Exists;
        }

        public virtual bool AliasDoesNotExistOnIndex(string alias, string index)
        {
            return !AliasExistsOnIndex(alias, index);
        }

        public virtual bool AliasExistsOnIndex(VersionedIndexName versionedIndex)
        {
            return AliasExistsOnIndex(versionedIndex.AliasName, versionedIndex.IndexNameWithVersion());
        }

        public virtual bool AliasDoesNotExistOnIndex(VersionedIndexName versionedIndex)
        {
            return AliasDoesNotExistOnIndex(versionedIndex.AliasName, versionedIndex.IndexNameWithVersion());
        }
    }
}