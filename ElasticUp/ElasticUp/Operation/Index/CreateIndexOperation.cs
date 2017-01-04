using System;
using System.Collections.Generic;
using ElasticUp.Alias;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Index
{
    public class CreateIndexOperation : AbstractElasticUpOperation
    {
        protected string IndexName;
        protected string Alias;

        protected Func<MappingsDescriptor, IPromise<IMappings>> MappingSelector;
        protected CreateIndexDescriptor CreateIndexDescriptor;


        public CreateIndexOperation(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
        }

        public virtual CreateIndexOperation WithAlias(string alias)
        {
            Alias = alias?.ToLowerInvariant();
            return this;
        }

        public virtual CreateIndexOperation WithMapping(Func<MappingsDescriptor, IPromise<IMappings>> selector)
        {


            MappingSelector = selector;
            return this;
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"CreateIndexOperation: Invalid indexName {IndexName}");
            if (MappingSelector == null) throw new ElasticUpException($"CreateIndexOperation: Invalid mapping (required)");
            if (!string.IsNullOrEmpty(Alias) && string.IsNullOrWhiteSpace(Alias)) throw new ElasticUpException($"CreateIndexOperation: Invalid alias {Alias}");
            if (elasticClient.IndexExists(IndexName).Exists) throw new ElasticUpException($"CreateIndexOperation: index {IndexName} already exists.");   
            
            var descriptor = new CreateIndexDescriptor(IndexName);
            descriptor.Mappings(MappingSelector);
            
            if (!string.IsNullOrWhiteSpace(Alias)) { descriptor.Aliases(aliases => aliases.Alias(Alias)); }

            elasticClient.CreateIndex(descriptor);
        }
    }
}
