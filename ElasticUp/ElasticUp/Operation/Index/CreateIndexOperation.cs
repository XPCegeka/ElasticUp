using System;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Index
{
    public class CreateIndexOperation : AbstractElasticUpOperation
    {
        protected string IndexName;
        protected string Alias;

        protected Func<MappingsDescriptor, IPromise<IMappings>> MappingSelector;
        protected Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> SettingSelector;
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

        public virtual CreateIndexOperation WithIndexSettings(Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> selector)
        {
            SettingSelector = selector;
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<CreateIndexOperation>()
                .IsNotBlank(Alias, RequiredMessage("Alias"))
                .IsNotBlank(IndexName, RequiredMessage("IndexName"));

            IndexValidationsFor<CreateIndexOperation>(elasticClient)
                .IndexDoesNotExists(IndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            var descriptor = new CreateIndexDescriptor(IndexName);
            
            descriptor.Mappings(MappingSelector);
            descriptor.Settings(SettingSelector);
            
            if (!string.IsNullOrWhiteSpace(Alias)) { descriptor.Aliases(aliases => aliases.Alias(Alias)); }

            elasticClient.CreateIndex(descriptor);
        }
    }
}
