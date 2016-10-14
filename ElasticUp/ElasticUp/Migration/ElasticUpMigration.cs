using System;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        private readonly int _migrationNumber;
        public string IndexAlias { get; protected set; }

        protected ElasticUpMigration(int migrationNumber)
        {
            _migrationNumber = migrationNumber;
        }

        public ElasticUpMigration OnIndexAlias(string indexAlias)
        {
            IndexAlias = indexAlias;
            return this;
        }

        public void Execute(IElasticClient elasticClient)
        {
            var indicesForAlias = elasticClient.GetIndicesPointingToAlias(IndexAlias);

            foreach (var indexForAlias in indicesForAlias)
            {
                var indexName = VersionedIndexName.CreateFromIndexName(indexForAlias);
                var nextIndexName = indexName.GetIncrementedVersion();

                // TODO execute operations for each index
            }
        }
    }
}
