using System;
using ElasticUp.Helper;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Validations
{
    public class IndexValidations
    {
        private readonly Type _type;
        private readonly IndexHelper _indexHelper;
        private readonly AliasHelper _aliasHelper;

        private string ExceptionMessage(string message) => $"{_type.Name}: {message}";

        private IndexValidations(IElasticClient elasticClient, Type type)
        {
            _type = type;
            _indexHelper = new IndexHelper(elasticClient);
            _aliasHelper = new AliasHelper(elasticClient);
        }

        public static IndexValidations IndexValidationsFor<T>(IElasticClient elasticClient) where T : class
        {
            return new IndexValidations(elasticClient, typeof(T));
        }

        public IndexValidations IndexExists(string index)
        {
            if (_indexHelper.IndexDoesNotExist(index)) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            return this;
        }

        public IndexValidations IndexDoesNotExists(string index)
        {
            if (_indexHelper.IndexExists(index)) throw new ElasticUpException(ExceptionMessage($"Index '{index}' already exists"));
            return this;
        }

        public IndexValidations IndexExists(VersionedIndexName versionedIndexName)
        {
            return IndexExists(versionedIndexName.IndexNameWithVersion());
        }

        public IndexValidations IndexDoesNotExists(VersionedIndexName versionedIndexName)
        {
            return IndexDoesNotExists(versionedIndexName.IndexNameWithVersion());
        }

        public IndexValidations IndexExistsWithAlias(VersionedIndexName versionedIndexName)
        {
            return IndexExistsWithAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
        }

        public IndexValidations IndexExistsWithAlias(string index, string alias)
        {
            if (_indexHelper.IndexDoesNotExist(index)) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            if (_aliasHelper.AliasDoesNotExistOnIndex(alias, index)) throw new ElasticUpException(ExceptionMessage($"Alias '{alias}' does not exist on index '{index}'"));
            return this;
        }

        public IndexValidations IndexExistsWithoutAlias(VersionedIndexName versionedIndexName)
        {
            return IndexExistsWithoutAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
        }

        public IndexValidations IndexExistsWithoutAlias(string index, string alias)
        {
            if (_indexHelper.IndexDoesNotExist(index)) throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
            if (_aliasHelper.AliasExistsOnIndex(alias, index)) throw new ElasticUpException(ExceptionMessage($"Alias '{alias}' does not exist on index '{index}'"));
            return this;
        }
    }
}