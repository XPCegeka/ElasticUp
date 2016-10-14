using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        private IElasticClient _elasticClient;
        public readonly IList<ElasticUpMigration> Migrations = new List<ElasticUpMigration>();
        
        private ElasticUpRunner(IElasticClient elasticClient)
        {
            this._elasticClient = elasticClient;
        }

        public static ElasticUpRunner CreateElasticUpRunner(IElasticClient elasticClient)
        {
            return new ElasticUpRunner(elasticClient);
        }

        public ElasticUpRunner Migration(ElasticUpMigration migration)
        {
            AssertMigrationNumber(migration.MigrationNumber);
            Migrations.Add(migration);
            return this;
        }

        public void Run()
        {
            //TODO 
        }

        private void AssertMigrationNumber(int newMigrationNumber)
        {
            var migrationNumbers = Migrations.Select(migration => migration.MigrationNumber).ToList();

            if (migrationNumbers.Any(number => number >= newMigrationNumber))
                throw new ArgumentException("Your migrations should have unique ascending numbers!");
        }

    }
}
