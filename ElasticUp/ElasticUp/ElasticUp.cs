using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp
{
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
