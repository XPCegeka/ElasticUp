using System;
using System.Collections.Generic;
using System.Diagnostics;
using ElasticUp.Extension;
using ElasticUp.Migration;
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
                migration.Execute(_elasticClient);
                stopwatch.Stop();
                Console.WriteLine($"Finished ElasticUp migration: {migration.ToString()} in {stopwatch.Elapsed.ToHumanTimeString()}");


                // TODO alias stuff per migration
                //TODO add this migration to MigrationHistory in new index ?


            }


            Console.WriteLine("Finished ElasticUp migrations");
            throw new NotImplementedException();
        }
    }
}
