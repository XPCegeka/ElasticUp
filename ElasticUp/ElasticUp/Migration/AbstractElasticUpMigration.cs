using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ElasticUp.Alias;
using ElasticUp.Extension;
using ElasticUp.History;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class AbstractElasticUpMigration
    {
        public IElasticClient ElasticClient { get; set; }
        public MigrationHistoryHelper MigrationHistoryHelper { get; set; }
        public AliasHelper AliasHelper { get; set; }

        public List<ElasticUpOperation> Operations { get; } = new List<ElasticUpOperation>();

        protected string SourceIndex;
        protected string TargetIndex;

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
            if (MigrationHistoryHelper.HasMigrationAlreadyBeenApplied(this))
            {
                Console.WriteLine($"Skipping migration: {this}. Already applied according to index: {MigrationHistoryHelper.MigrationHistoryIndexName}");
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
            Console.WriteLine($"Adding ElasticUp Migration: {migration} to MigrationHistory index: ({MigrationHistoryHelper.MigrationHistoryIndexName})");
            MigrationHistoryHelper.AddMigrationToHistory(migration);
        }

        protected virtual void MoveAlias(string alias, string sourceIndex, string targetIndex)
        {
            RemoveAlias(alias, sourceIndex);
            PutAlias(alias, targetIndex);
        }

        protected virtual void PutAlias(string alias, string index)
        {
            AliasHelper.PutAliasOnIndex(alias, index);
        }

        protected virtual void RemoveAlias(string alias, string index)
        {
            AliasHelper.RemoveAliasFromIndex(alias, index);
        }

        public void Operation(ElasticUpOperation operation)
        {
            Operations.Add(operation);
        }
        
        public sealed override string ToString()
        {
            return GetType().Name.ToLowerInvariant();
        }
    }
}
