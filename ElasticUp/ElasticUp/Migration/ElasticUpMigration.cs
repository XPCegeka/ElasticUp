using System;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        public int MigrationNumber { get; }
        public string IndexAlias { get; protected set; }

        protected ElasticUpMigration(int migrationNumber)
        {
            MigrationNumber = migrationNumber;
        }

        public override string ToString()
        {
            return $"{MigrationNumber}_{this.GetType().Name}";
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
