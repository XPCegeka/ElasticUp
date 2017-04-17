using System;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class ElasticUpVersionedIndexMigration : AbstractElasticUpMigration
    {
        protected readonly string Alias;
        protected string FromIndexName;
        protected string ToIndexName;

        protected ElasticUpVersionedIndexMigration(string alias)
        {
            if (string.IsNullOrWhiteSpace(alias)) throw new ElasticUpException($"CreateAliasOperation: Invalid alias {alias}");
            Alias = alias;
        }

        protected sealed override void BeforeMigration()
        {
            var indicesForAlias = ElasticClient.GetIndicesPointingToAlias(Alias);
            if (indicesForAlias == null || indicesForAlias.Count > 1)
                throw new NotImplementedException("Error: Not supporting multiple ElasticSearch indices with the same alias yet!");

            var versionedIndexName = VersionedIndexName.CreateFromIndexName(indicesForAlias[0]);

            FromIndexName = versionedIndexName.IndexNameWithVersion();
            ToIndexName = versionedIndexName.NextVersion().IndexNameWithVersion();
        }

        protected sealed override void AfterMigration()
        {
            AddMigrationToHistory(this);
            SwitchAlias(Alias, FromIndexName, ToIndexName);
        }
    }
}
