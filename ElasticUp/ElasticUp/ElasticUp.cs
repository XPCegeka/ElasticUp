using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Alias;
using ElasticUp.History;
using ElasticUp.Migration;
using ElasticUp.Util;
using Nest;

namespace ElasticUp
{
    public class ElasticUp
    {
        private readonly IElasticClient _elasticClient;
        private string _migrationHistoryIndexAliasName = typeof(ElasticUpMigrationHistory).Name.ToLowerInvariant();

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
            migration.MigrationHistoryHelper = new MigrationHistoryHelper(_elasticClient, _migrationHistoryIndexAliasName);
            migration.AliasHelper = new AliasHelper(_elasticClient);

            _migrations.Add(migration);
            return this;
        }

        private void AssertNoDoubles(AbstractElasticUpMigration newMigration)
        {
            if (_migrations.Any(migration => migration.ToString() == newMigration.ToString()))
                throw new ArgumentException("Your migrations should have a unique name!");
        }

        public ElasticUp WithMigrationHistoryIndexAliasName(string migrationHistoryIndexAliasName)
        {
            _migrationHistoryIndexAliasName = migrationHistoryIndexAliasName.ToLowerInvariant();
            return this;
        }

        public void Run()
        {
            Console.WriteLine($"Initializing ElasticUpMigrationHistory index: {_migrationHistoryIndexAliasName}");
            new MigrationHistoryHelper(_elasticClient, _migrationHistoryIndexAliasName).InitMigrationHistory();

            Console.WriteLine("Starting ElasticUp migrations");
            foreach (var migration in _migrations)
            {
                migration.Run();
            }
            Console.WriteLine("Finished ElasticUp migrations");
        }

    }
}
