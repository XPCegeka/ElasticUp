using System;
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

        public virtual void RemoveAliasOnIndices(string alias, params string[] indexNames)
        {
            var deleteAliasResponse = _elasticClient.DeleteAlias(Indices.Parse(string.Join(",", indexNames)), alias);
            if (!deleteAliasResponse.IsValid)
                throw new Exception($"DeleteAlias failed. Reason: '{deleteAliasResponse.DebugInformation}'");
        }

        public virtual void AddAliasOnIndices(string alias, params string[] indexNames)
        {
            var putAliasResponse = _elasticClient.PutAlias(Indices.Parse(string.Join(",", indexNames)), alias);
            if (!putAliasResponse.IsValid)
                throw new Exception($"PutAlias failed. Reason: ''{putAliasResponse.DebugInformation}");
        }
    }
}