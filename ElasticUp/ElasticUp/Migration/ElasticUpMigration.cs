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

        internal ElasticUpMigration(int migrationNumber)
        {
            MigrationNumber = migrationNumber;
            Operations = new List<ElasticUpOperation>();
        }

        internal void Operation(ElasticUpOperation operation)
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
            var indicesForAlias = elasticClient.GetIndicesPointingToAlias(IndexAlias);

            foreach (var indexForAlias in indicesForAlias)
            {
                var indexName = VersionedIndexName.CreateFromIndexName(indexForAlias);
                var nextIndexName = indexName.GetIncrementedVersion();

                Operations.ForEach(o => o.From(fromIndex).To(toIndex).Execute());
            }
        }

        private bool HasDuplicateOperationNumber(ElasticUpOperation operation)
        {
            return Operations.Any(o => o.OperationNumber == operation.OperationNumber);
        }
    }
}
