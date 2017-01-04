using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        public List<AbstractElasticUpOperation> Operations { get; } = new List<AbstractElasticUpOperation>();
        
        public virtual void Run()
        {
            if (SkipMigration()) return;

            BeforeMigrationHook();
            BeforeMigration();
            DefineOperations();
            RunMigration();
            AfterMigration();
            AfterMigrationHook();
        }
        
        protected virtual void BeforeMigrationHook() { }
        protected virtual void BeforeMigration() { }
        protected abstract void DefineOperations();
        protected virtual void AfterMigration() { }
        protected virtual void AfterMigrationHook() { }
        
        protected virtual bool SkipMigration()
        {
            if (!MigrationHistoryHelper.HasMigrationAlreadyBeenApplied(this)) return false;

            Console.WriteLine($"Skipping migration: {this}. Already applied according to index: {MigrationHistoryHelper.MigrationHistoryIndexAlias}");
            return true;
        }

        protected virtual void RunMigration()
        {
            Console.WriteLine($"Starting ElasticUp migration {this}");
            var stopwatch = Stopwatch.StartNew();

            Operations.ForEach(operation =>
            {
                Console.Write($"Running Operation: {operation} ");
                var operationStopwatch = Stopwatch.StartNew();
                operation.Execute(ElasticClient);
                operationStopwatch.Stop();
                Console.Write($"[Finished in {operationStopwatch.Elapsed.ToHumanTimeString()}]\n");
            });

            stopwatch.Stop();
            Console.WriteLine($"Finished ElasticUp migration {this} in {stopwatch.Elapsed.ToHumanTimeString()}]");
        }

        protected virtual void AddMigrationToHistory(AbstractElasticUpMigration migration)
        {
            Console.WriteLine($"Adding ElasticUp Migration: {migration} to MigrationHistory index: ({MigrationHistoryHelper.MigrationHistoryIndexAlias})");
            MigrationHistoryHelper.AddMigrationToHistory(migration);
        }

        protected virtual void SwitchAlias(string alias, string fromIndexName, string toIndexName)
        {
            AliasHelper.SwitchAlias(alias, fromIndexName, toIndexName);
        }

        protected virtual void PutAlias(string alias, string indexName)
        {
            AliasHelper.PutAliasOnIndex(alias, indexName);
        }

        protected virtual void RemoveAlias(string alias, string indexName)
        {
            AliasHelper.RemoveAliasFromIndex(alias, indexName);
        }

        public void Operation(AbstractElasticUpOperation operation)
        {
            Operations.Add(operation);
        }
        
        public sealed override string ToString()
        {
            return GetType().Name.ToLowerInvariant();
        }
    }
}
