using System;
using System.Collections.Generic;
using System.Diagnostics;
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
                
                //TODO alias stuff per migration
                //TODO add this migration to MigrationHistory in new index ?
                // alias stuff per migration
                // remove alias on old indices
                // add alias to new indices

            }
            Console.WriteLine("Finished ElasticUp migrations");
        }

        private void Migrate(string indexName, ElasticUpMigration migration)
        {
            var fromIndex = VersionedIndexName.CreateFromIndexName(indexName);
            var toIndex = fromIndex.GetIncrementedVersion();
            var migrationHistoryService = new MigrationHistoryService(_elasticClient, fromIndex, toIndex);

            if (migrationHistoryService.HasMigrationAlreadyBeenApplied(migration))
            {
                Console.WriteLine($"Already executed operation: {migration.ToString()} on old index {fromIndex}. Not migrating to new index {toIndex.ToString()}");
                return;
            }

            Console.WriteLine($"Copying ElasticUp MigrationHistory to new index: {toIndex.ToString()}");
            migrationHistoryService.CopyMigrationHistory();

            Console.WriteLine($"Starting ElasticUp migration: {migration.ToString()} to new index: {toIndex.ToString()}");
            var stopwatch = Stopwatch.StartNew();
            migration.Execute(_elasticClient, fromIndex, toIndex);
            stopwatch.Stop();
            Console.WriteLine($"Finished ElasticUp migration: {migration.ToString()} to new index: {toIndex.ToString()} in {stopwatch.Elapsed.ToHumanTimeString()}");

            Console.WriteLine($"Adding ElasticUp Migration: {migration.ToString()} to MigrationHistory of new index: {toIndex.ToString()}");
            migrationHistoryService.AddMigrationToHistory(migration);
        }
    }
}
