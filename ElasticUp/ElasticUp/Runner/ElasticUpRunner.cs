using System;
using System.Collections.Generic;
using System.Diagnostics;
using ElasticUp.Extension;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        private readonly IElasticClient _elasticClient;
        private MigrationHistoryService _migrationHistoryService;

        public ElasticUpRunner(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
            _migrationHistoryService = new MigrationHistoryService(_elasticClient);
        }

        public void Execute(List<ElasticUpMigration> migrations)
        {
            Console.WriteLine("Starting ElasticUp migrations");

            //TODO copy MigrationHistory to new index ?

            foreach (var migration in migrations)
            {
                if (_migrationHistoryService.HasMigrationAlreadyBeenApplied(migration))
                {
                    Console.WriteLine($"Already executed operation: {migration.ToString()}");
                    return;
                }

                Console.WriteLine($"Starting ElasticUp operation: {migration.ToString()}");
                var stopwatch = Stopwatch.StartNew();

                var index0 = new VersionedIndexName("test", 0);
                var index1 = index0.GetIncrementedVersion();

                migration.Execute(_elasticClient, index0, index1);
                stopwatch.Stop();
                Console.WriteLine($"Finished ElasticUp migration: {migration.ToString()} in {stopwatch.Elapsed.ToHumanTimeString()}");
                

                // TODO alias stuff per migration
                //TODO add this migration to MigrationHistory in new index ?
                // alias stuff per migration
                // remove alias on old indices
                // add alias to new indices

            }


            Console.WriteLine("Finished ElasticUp migrations");
            throw new NotImplementedException();
        }
    }
}
