using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Alias;
using ElasticUp.Elastic;
using ElasticUp.History;
using ElasticUp.Migration;
using Nest;


namespace ElasticUp
{
    /*
     TODO ElasticUp:
     - MigrationHistory in it's own index and allow configuration of index name (default ElasticUpMigrationHistory)
     - DONE elasticclient connection settings kunnen automatisch exception gooien ipv ElasticClientHelper
     - DONE moet elasticclient niet zelf geinstantieerd worden door elasticup en gewoon url meegeven ?        
     - DONE copy type operation rename to ReindexOperation 
     - reindexoperation:  allow scripts to be passed in
     - scroll searches telkens nieuwe scrollId gebruiken EN scroll met sort _doc (performance)
     - only support elastic 5+ : maak gebruik van sliced scroll search
     - DONE tests starten nu telkens opnieuw elastic -> elke test eigen index, in setup wordt elasticclient aangemaakt en daar defaultindex zetten
     - DONE unittest indexes : 1 shard 0 replicas

     - LOW PRIO: make ElasticUp in dotnetcore met support voor 4.5.x ! 
    */

    public class ElasticUp
    {
        private readonly IElasticClient _elasticClient;
        private string _migrationHistoryIndexName = typeof(ElasticUpMigrationHistory).Name.ToLowerInvariant();

        private readonly List<AbstractElasticUpMigration> _migrations = new List<AbstractElasticUpMigration>();

        public ElasticUp(string elasticSearchUrl)
        {
            var settings = new ConnectionSettings(new Uri(elasticSearchUrl)).ThrowExceptions(true);
            _elasticClient = new ElasticClient(settings);
        }

        public ElasticUp(IElasticClient elasticClient)
        {
            if (!elasticClient.ConnectionSettings.ThrowExceptions) { throw new ElasticUpException("ElasticUp requires your ElasticClient to have ConnectionSettings.ThrowExceptions set to true"); }
            _elasticClient = elasticClient;
        }
        
        public ElasticUp Migration(AbstractElasticUpMigration migration)
        {
            AssertNoDoubles(migration);

            migration.ElasticClient = _elasticClient;
            migration.MigrationHistoryHelper = new MigrationHistoryHelper(_elasticClient, _migrationHistoryIndexName);
            migration.AliasHelper = new AliasHelper(_elasticClient);

            _migrations.Add(migration);
            return this;
        }

        private void AssertNoDoubles(AbstractElasticUpMigration newMigration)
        {
            if (_migrations.Any(migration => migration.ToString() == newMigration.ToString()))
                throw new ArgumentException("Your migrations should have a unique name!");
        }

        public ElasticUp WithMigrationHistoryIndexName(string migrationHistoryIndexName)
        {
            _migrationHistoryIndexName = migrationHistoryIndexName.ToLowerInvariant();
            return this;
        }

        public void Run()
        {
            Console.WriteLine("Starting ElasticUp migrations");
            foreach (var migration in _migrations)
            {
                migration.Run();
            }
            Console.WriteLine("Finished ElasticUp migrations");
        }

    }
}
