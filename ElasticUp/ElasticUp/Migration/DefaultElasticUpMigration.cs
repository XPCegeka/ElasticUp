using System;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class DefaultElasticUpMigration : AbstractElasticUpMigration
    {
        protected string IndexAlias;

        protected DefaultElasticUpMigration(IElasticClient elasticClient, string indexAlias) : base(elasticClient)
        {
            IndexAlias = indexAlias;
        }

        public override void PreMigrationTasks()
        {
            SetIndices();
            // check if migration has already run
            // copy history from index to index
            
        }

        public override void PostMigrationTasks()
        {
            // add this migration to history 
            // move alias
        }

        private void SetIndices()
        {
            var indicesForAlias = ElasticClient.GetIndicesPointingToAlias(IndexAlias);
            if (indicesForAlias == null || indicesForAlias.Count > 1)
                throw new NotImplementedException("Error: Not supporting multiple ElasticSearch indices with the same alias yet!");

            var versionedIndexName = VersionedIndexName.CreateFromIndexName(indicesForAlias[0]);

            SourceIndex = versionedIndexName.ToString();
            TargetIndex = versionedIndexName.GetIncrementedVersion().ToString();
        }
    }
}
