using System;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Validations
{
    public class IndexValidations
    {
        private readonly Type _type;
        private readonly IElasticClient _elasticClient;

        private string ExceptionMessage(string message) => $"{_type.Name}: {message}";

        private IndexValidations(IElasticClient elasticClient, Type type)
        {
            _type = type;
            _elasticClient = elasticClient;
        }

        public static IndexValidations IndexValidationsFor<T>(IElasticClient elasticClient) where T : class
        {
            return new IndexValidations(elasticClient, typeof(T));
        }

        public IndexValidations IndexExists(string index)
        {
            if (!_elasticClient.IndexExists(index).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            return this;
        }

        public IndexValidations IndexDoesNotExists(string index)
        {
            if (_elasticClient.IndexExists(index).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{index}' already exists"));
            return this;
        }

        public IndexValidations IndexExists(VersionedIndexName versionedIndexName)
        {
            if (!_elasticClient.IndexExists(versionedIndexName.IndexNameWithVersion()).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{versionedIndexName.IndexNameWithVersion()}' does not exist"));
            return this;
        }

        public IndexValidations IndexDoesNotExists(VersionedIndexName versionedIndexName)
        {
            if (_elasticClient.IndexExists(versionedIndexName.IndexNameWithVersion()).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{versionedIndexName.IndexNameWithVersion()}' already exist"));
            return this;
        }

        public IndexValidations IndexExistsWithAlias(VersionedIndexName versionedIndexName)
        {
            IndexExistsWithAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
            return this;
        }

        public IndexValidations IndexExistsWithAlias(string index, string alias)
        {
            if (!_elasticClient.IndexExists(index).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            if (!_elasticClient.AliasExists(r => r.Index(index).Name(alias)).Exists) throw new ElasticUpException(ExceptionMessage($"Alias '{alias}' does not exist on index '{index}'"));
            return this;
        }

        public IndexValidations IndexExistsWithoutAlias(VersionedIndexName versionedIndexName)
        {
            IndexExistsWithoutAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
            return this;
        }

        public IndexValidations IndexExistsWithoutAlias(string index, string alias)
        {
            if (!_elasticClient.IndexExists(index).Exists) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            if (_elasticClient.AliasExists(r => r.Index(index).Name(alias)).Exists) throw new ElasticUpException(ExceptionMessage($"Alias '{alias}' should not exist on index '{index}'"));
            return this;
        }
    }
}