using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Operation;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        internal int MigrationNumber { get; }
        internal List<ElasticUpOperation> Operations { get; }
        public string IndexAlias { get; protected set; }

        protected ElasticUpMigration(int migrationNumber)
        {
            MigrationNumber = migrationNumber;
            Operations = new List<ElasticUpOperation>();
        }

        public void Operation(ElasticUpOperation operation)
        {
            if(HasDuplicateOperationNumber(operation)) 
                throw new ArgumentException("Duplicate operation number.");

            Operations.Add(operation);
        }

        public sealed override string ToString()
        {
            return $"{MigrationNumber:D3}_{this.GetType().Name}";
        }

        public ElasticUpMigration OnIndexAlias(string indexAlias)
        {
            IndexAlias = indexAlias;
            return this;
        }

        internal void Execute(IElasticClient elasticClient, VersionedIndexName fromIndex, VersionedIndexName toIndex)
        {
            Operations.ForEach(o => o.Execute(elasticClient, fromIndex.ToString(), toIndex.ToString()));
        }

        private bool HasDuplicateOperationNumber(ElasticUpOperation operation)
        {
            return Operations.Any(o => o.OperationNumber == operation.OperationNumber);
        }
    }
}
