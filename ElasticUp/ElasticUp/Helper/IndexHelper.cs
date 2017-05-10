using ElasticUp.Util;
using Nest;

namespace ElasticUp.Helper
{
    public class IndexHelper
    {
        private readonly IElasticClient _elasticClient;
        private readonly AliasHelper _aliasHelper;

        public IndexHelper(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
            _aliasHelper = new AliasHelper(_elasticClient);
        }

        public bool IndexExists(string index)
        {
            return _elasticClient.IndexExists(index).Exists;
        }

        public bool IndexDoesNotExist(string index)
        {
            return !IndexExists(index);
        }

        public bool IndexExists(VersionedIndexName versionedIndexName)
        {
            return IndexExists(versionedIndexName.IndexNameWithVersion());
        }

        public bool IndexDoesNotExist(VersionedIndexName versionedIndexName)
        {
            return IndexDoesNotExist(versionedIndexName.IndexNameWithVersion());
        }

        //TODO write tests for these methods
        public bool IndexExistsWithAlias(VersionedIndexName versionedIndexName)
        {
            return IndexExistsWithAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
        }

        public bool IndexExistsWithAlias(string index, string alias)
        {
            return IndexExists(index) && _aliasHelper.AliasExistsOnIndex(alias, index);
        }

        public bool IndexExistsWithoutAlias(VersionedIndexName versionedIndexName)
        {
            return IndexExistsWithoutAlias(versionedIndexName.IndexNameWithVersion(), versionedIndexName.AliasName);
        }

        public bool IndexExistsWithoutAlias(string index, string alias)
        {
            return IndexExists(index) && _aliasHelper.AliasDoesNotExistOnIndex(alias, index);
        }
    }
}
