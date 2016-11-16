using System;
using ElasticUp.Migration;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.History
{
    public class MigrationHistoryService
    {
        private readonly IElasticClient _elasticClient;

        public MigrationHistoryService(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            if (string.IsNullOrEmpty(fromIndex))
                throw new ArgumentNullException(nameof(fromIndex));
            if (string.IsNullOrEmpty(toIndex))
                throw new ArgumentNullException(nameof(toIndex));

            var copyTypeOperation = new CopyTypeOperation<MigrationHistory>(0);
            copyTypeOperation.Execute(_elasticClient, fromIndex, toIndex);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, string indexName)
        {
            AddMigrationToHistory(migration?.ToString(), indexName, null);
        }

        public void AddMigrationToHistory(ElasticUpMigration migration, string indexName)
        {
            AddMigrationToHistory(migration?.ToString(), indexName, null);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, string indexName, Exception exception)
        {
            AddMigrationToHistory(migration?.ToString(), indexName, exception);
        }

        public void AddMigrationToHistory(string migrationName, string indexName, Exception exception)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            var history = new MigrationHistory(migrationName, exception);

            _elasticClient.Index(history, descriptor => descriptor.Index(indexName));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration, string indexName)
        {
            return HasMigrationAlreadyBeenApplied(migration.ToString(), indexName);
        }

        public bool HasMigrationAlreadyBeenApplied(ElasticUpMigration migration, string indexName)
        {
            return HasMigrationAlreadyBeenApplied(migration.ToString(), indexName);
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName, string indexName)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            var existsResponse = _elasticClient.Get<MigrationHistory>(migrationName, descriptor => descriptor.Index(indexName));

            if (existsResponse.ServerError != null)
                throw new Exception($"Could not verify if migration '{migrationName}' was already applied. Debug information: '{existsResponse.DebugInformation}'");

            return existsResponse.Found && existsResponse.Source.HasBeenAppliedSuccessfully;
        }
    }
}

