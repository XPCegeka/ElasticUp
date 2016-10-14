using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Operation;

namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        internal int MigrationNumber { get; }
        internal List<ElasticUpOperation> Operations { get; }

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

        internal void Execute()
        {
            Operations.ForEach(o => o.Execute());
        }

        private bool HasDuplicateOperationNumber(ElasticUpOperation operation)
        {
            return Operations.Any(o => o.OperationNumber == operation.OperationNumber);
        }
    }
}
