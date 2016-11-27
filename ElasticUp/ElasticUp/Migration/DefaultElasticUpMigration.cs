using System;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class DefaultElasticUpMigration : AbstractElasticUpMigration
    {
        protected readonly string IndexAlias;

        protected DefaultElasticUpMigration(int migrationNumber, string indexAlias) : base(migrationNumber)
        {
            IndexAlias = indexAlias;
        }

        protected override void InitIndices()
        {
            var indicesForAlias = ElasticClient.GetIndicesPointingToAlias(IndexAlias);
            if (indicesForAlias == null || indicesForAlias.Count > 1)
                throw new NotImplementedException("Error: Not supporting multiple ElasticSearch indices with the same alias yet!");

            var versionedIndexName = VersionedIndexName.CreateFromIndexName(indicesForAlias[0]);

            SourceIndex = versionedIndexName.IndexNameWithVersion();
            TargetIndex = versionedIndexName.NextVersion().IndexNameWithVersion();
        }

        protected override void PreMigrationTasks()
        {
            CopyHistory(SourceIndex, TargetIndex);
        }

        protected override void PostMigrationTasks()
        {
            AddMigrationToHistory(this, TargetIndex);
            MoveAlias(IndexAlias, SourceIndex, TargetIndex);
        }
    }
}
