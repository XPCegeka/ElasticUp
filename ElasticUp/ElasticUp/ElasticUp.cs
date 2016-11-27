using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Migration;
using Nest;


namespace ElasticUp
{
    /*
     TODO ElasticUp:
     - MigrationHistory in it's own index and allow configuration of index name (default ElasticUpMigrationHistory)
     - elasticclient connection settings kunnen automatisch exception gooien ipv ElasticClientHelper
     - moet elasticclient niet zelf geinstantieerd worden door elasticup en gewoon url meegeven ?        
     - copy type operation rename to ReindexOperation and allow scripts to be passed in
     - scroll searches telkens nieuwe scrollId gebruiken EN scroll met sort _doc (performance)
     - only support elastic 5+ : maak gebruik van sliced scroll search
     - tests starten nu telkens opnieuw elastic -> elke test eigen index, in setup wordt elasticclient aangemaakt en daar defaultindex zetten
     - unittest indexes : 1 shard 0 replicas
     - 
     - LOW PRIO: make ElasticUp in dotnetcore met support voor 4.5.x ! 
    */

    public class ElasticUp
    {
        private readonly IElasticClient _elasticClient;
        public readonly List<AbstractElasticUpMigration> Migrations = new List<AbstractElasticUpMigration>();
        
        public ElasticUp(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }
        
        public ElasticUp Migration(AbstractElasticUpMigration migration)
        {
            AssertNoDoubles(migration);
            migration.SetElasticClient(_elasticClient);
            Migrations.Add(migration);
            return this;
        }

        private void AssertNoDoubles(AbstractElasticUpMigration newMigration)
        {
            var migrationNumbers = Migrations.Select(migration => migration.MigrationNumber).ToList();

            if (migrationNumbers.Any(number => number >= newMigration.MigrationNumber))
                throw new ArgumentException("Your migrations should have unique ascending numbers!");
        }

        public void Run()
        {
            Console.WriteLine("Starting ElasticUp migrations");
            foreach (var migration in Migrations)
            {
                migration.Run();
            }
            Console.WriteLine("Finished ElasticUp migrations");
        }

    }
}
