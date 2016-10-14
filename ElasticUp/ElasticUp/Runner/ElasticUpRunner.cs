using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

            foreach (var migration in migrations)
            {
                if (_migrationHistoryService.HasOperationBeenApplied(migration))
                {
                    Console.WriteLine($"Already executed migration {migration.ToString()}");
                    return;
                }

                //For each migration
                //For each operation
                // check if operation already ran
                //  if yes skip
                //  if no execute

                // alias stuff per migration


            }



            throw new NotImplementedException();
        }
    }
}
