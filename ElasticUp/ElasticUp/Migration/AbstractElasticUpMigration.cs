using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ElasticUp.Alias;
using ElasticUp.Extension;
using ElasticUp.History;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class AbstractElasticUpMigration
    {
        protected IElasticClient ElasticClient;
        protected MigrationHistoryHelper MigrationHistoryHelper;
        protected AliasHelper AliasHelper;

        public List<ElasticUpOperation> Operations { get; } = new List<ElasticUpOperation>();

        protected string SourceIndex;
        protected string TargetIndex;
        internal int MigrationNumber { get; }

        public AbstractElasticUpMigration(int migrationNumber)
        {
            this.MigrationNumber = migrationNumber;
        }

        public void SetElasticClient(IElasticClient elasticClient)
        {
            ElasticClient = elasticClient;
            MigrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            AliasHelper = new AliasHelper(ElasticClient);
        }

        public virtual void Run()
        {
            InitIndices();

            if (SkipMigration()) return;

            PreMigrationTasks();

            Console.WriteLine($"Starting ElasticUp migration {this} from index {SourceIndex} to index {TargetIndex}");
            var stopwatch = Stopwatch.StartNew();
            DoMigrationTasks();
            stopwatch.Stop();
            Console.WriteLine($"Finished ElasticUp migration {this} from index {SourceIndex} to index {TargetIndex} in {stopwatch.Elapsed.ToHumanTimeString()}");
            
            PostMigrationTasks();
        }

        protected abstract void InitIndices();
        protected abstract void PreMigrationTasks();
        protected abstract void PostMigrationTasks();

        protected virtual bool SkipMigration()
        {
            if (MigrationHistoryHelper.HasMigrationAlreadyBeenApplied(this, SourceIndex))
            {
                Console.WriteLine($"Already ran migration {this} on old index {SourceIndex}. Not migrating to new index {TargetIndex}");
                return true;
            }
            return false;
        }

        protected virtual void CopyHistory(string sourceIndex, string targetIndex)
        {
            Console.WriteLine($"Copying ElasticUp MigrationHistory from index {sourceIndex} to index {targetIndex}");
            MigrationHistoryHelper.CopyMigrationHistory(sourceIndex, targetIndex);
        }

        protected virtual void DoMigrationTasks()
        {
            Operations.ForEach(o => o.Execute(ElasticClient, SourceIndex, TargetIndex));
        }

        protected virtual void AddMigrationToHistory(AbstractElasticUpMigration migration, string index)
        {
            Console.WriteLine($"Adding ElasticUp Migration {migration} to MigrationHistory of index {index}");
            MigrationHistoryHelper.AddMigrationToHistory(migration, index);
        }

        protected virtual void MoveAlias(string alias, string sourceIndex, string targetIndex)
        {
            RemoveAlias(alias, sourceIndex);
            PutAlias(alias, targetIndex);
        }

        protected virtual void PutAlias(string alias, string index)
        {
            AliasHelper.AddAliasOnIndices(alias, index);
        }

        protected virtual void RemoveAlias(string alias, string index)
        {
            AliasHelper.RemoveAliasOnIndices(alias, index);
        }

        public void Operation(ElasticUpOperation operation)
        {
            if (HasDuplicateOperationNumber(operation)) throw new ArgumentException("Duplicate operation number.");
            Operations.Add(operation);
        }

        private bool HasDuplicateOperationNumber(ElasticUpOperation operation)
        {
            return Operations.Any(o => o.OperationNumber == operation.OperationNumber);
        }

        public sealed override string ToString()
        {
            return ($"{MigrationNumber:D3}_{this.GetType().Name}").ToLowerInvariant();
        }
    }
}
