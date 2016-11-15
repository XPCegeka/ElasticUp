using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ElasticUp.Extension;
using ElasticUp.History;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        private readonly IElasticClient _elasticClient;

        public ElasticUpRunner(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public void Execute(List<ElasticUpMigration> migrations)
        {
            Console.WriteLine("Starting ElasticUp migrations");

            foreach (var migration in migrations)
            {
                var indicesForAlias = _elasticClient.GetIndicesPointingToAlias(migration.IndexAlias);

                //TODO make possible to run in parallel?
                foreach (var indexName in indicesForAlias)
                {
                    Migrate(indexName, migration);
                }

                SetAliasesForMigration(migration, indicesForAlias);
            }
            Console.WriteLine("Finished ElasticUp migrations");
        }

        private void SetAliasesForMigration(ElasticUpMigration migration, IList<string> indicesForAlias)
        {
            var aliasHelper = new AliasHelper(_elasticClient);
            aliasHelper.RemoveAliasOnIndices(migration.IndexAlias, indicesForAlias.ToArray());
            var newIndices = indicesForAlias.Select(x => VersionedIndexName.CreateFromIndexName(x).GetIncrementedVersion().ToString());
            aliasHelper.AddAliasOnIndices(migration.IndexAlias, newIndices.ToArray());
        }

        private void Migrate(string indexName, ElasticUpMigration migration)
        {
            var fromIndex = VersionedIndexName.CreateFromIndexName(indexName);
            var toIndex = fromIndex.GetIncrementedVersion();
            var migrationHistoryService = new MigrationHistoryService(_elasticClient);

            if (migrationHistoryService.HasMigrationAlreadyBeenApplied(migration, fromIndex))
            {
                Console.WriteLine($"Already executed operation: {migration} on old index {fromIndex}. Not migrating to new index {toIndex}");
                return;
            }

            Console.WriteLine($"Copying ElasticUp MigrationHistory to new index: {toIndex}");
            migrationHistoryService.CopyMigrationHistory(fromIndex, toIndex);

            Console.WriteLine($"Starting ElasticUp migration: {migration} to new index: {toIndex}");
            var stopwatch = Stopwatch.StartNew();
            migration.Execute(_elasticClient, fromIndex, toIndex);
            stopwatch.Stop();
            Console.WriteLine($"Finished ElasticUp migration: {migration} to new index: {toIndex} in {stopwatch.Elapsed.ToHumanTimeString()}");

            Console.WriteLine($"Adding ElasticUp Migration: {migration} to MigrationHistory of new index: {toIndex}");
            migrationHistoryService.AddMigrationToHistory(migration, toIndex);
        }
    }
}
