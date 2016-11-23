using System;
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
            var indices = string.Join(",", indexNames);
            var removeAliasResponse = _elasticClient.Alias(
                descriptor => descriptor.Remove(removeDescriptor =>
                    removeDescriptor.Alias(alias).Index(indices)));

            if (!removeAliasResponse.IsValid)
                throw new Exception($"RemoveAlias failed. Could not remove alias '{alias}' from indices '{indices}'. Reason: '{removeAliasResponse.DebugInformation}'");
        }

        public virtual void AddAliasOnIndices(string alias, params string[] indexNames)
        {
            var putAliasResponse = _elasticClient.PutAlias(Indices.Parse(string.Join(",", indexNames)), alias);
            if (!putAliasResponse.IsValid)
                throw new Exception($"PutAlias failed. Reason: ''{putAliasResponse.DebugInformation}");
        }
    }
}