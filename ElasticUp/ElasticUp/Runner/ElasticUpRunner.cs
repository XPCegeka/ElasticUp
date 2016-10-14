using System;
using System.Collections.Generic;
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
                //TODO execute operation
                migration.Execute(_elasticClient);

                
                
                // TODO alias stuff per migration
                //TODO add this migration to MigrationHistory in new index ?

                Console.WriteLine($"Finished ElasticUp migration: {migration.ToString()}");
            }


            Console.WriteLine("Finished ElasticUp migrations");
            throw new NotImplementedException();
        }
    }
}
