using System;
using System.Collections.Generic;
using ElasticUp.Elastic;
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

        public virtual void RemoveAliasOnIndices(string alias, params string[] indexNames)
        {
            var indices = string.Join(",", indexNames);
            ElasticClientHelper.ValidateElasticResponse(
                _elasticClient.Alias(
                    descriptor => descriptor.Remove(removeDescriptor =>
                        removeDescriptor.Alias(alias).Index(indices))));
        }

        public virtual void AddAliasOnIndices(string alias, params string[] indexNames)
        {
            var putAliasResponse = _elasticClient.PutAlias(string.Join(",", indexNames), alias);
            if (!putAliasResponse.IsValid)
                throw new Exception($"PutAlias failed. Reason: ''{putAliasResponse.DebugInformation}");
        }
    }
}