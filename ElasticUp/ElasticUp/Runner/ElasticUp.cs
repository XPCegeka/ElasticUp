using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUp
    {
        private readonly IElasticClient _elasticClient;
        public readonly List<ElasticUpMigration> Migrations = new List<ElasticUpMigration>();
        
        private ElasticUp(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public static ElasticUp ConfigureElasticUp(IElasticClient elasticClient)
        {
            return new ElasticUp(elasticClient);
        }
        
        public ElasticUp Migration(ElasticUpMigration migration)
        {
            AssertMigrationNumber(migration.MigrationNumber);
            Migrations.Add(migration);
            return this;
        }

        private void AssertMigrationNumber(int newMigrationNumber)
        {
            var migrationNumbers = Migrations.Select(migration => migration.MigrationNumber).ToList();

            if (migrationNumbers.Any(number => number >= newMigrationNumber))
                throw new ArgumentException("Your migrations should have unique ascending numbers!");
        }

        public void Run()
        {
            new ElasticUpRunner(_elasticClient).Execute(Migrations);
            
        }

    }
}
