﻿using Elasticsearch.Net;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Mapping
{
    public class PutTypeMappingOperation : AbstractElasticUpOperation
    {
        protected string IndexName;
        protected string Type;
        protected string JsonMappingAsString;

        public PutTypeMappingOperation(string type)
        {
            Type = type?.ToLowerInvariant();
        }

        public virtual PutTypeMappingOperation OnIndex(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
            return this;
        }

        public virtual PutTypeMappingOperation WithMapping(string jsonMappingAsString)
        {
            JsonMappingAsString = jsonMappingAsString;
            return this;
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"PutTypeMappingOperation: Invalid indexName {IndexName}");
            if (string.IsNullOrWhiteSpace(Type)) throw new ElasticUpException($"PutTypeMappingOperation: Invalid type {Type}");
            if (string.IsNullOrWhiteSpace(JsonMappingAsString)) throw new ElasticUpException($"PutTypeMappingOperation: Invalid json mapping {JsonMappingAsString}");

            elasticClient.LowLevel.IndicesPutMapping<byte[]>(IndexName, Type, new PostData<object>(JsonMappingAsString));
        }
    }

    public class PutTypeMappingOperation<T> : PutTypeMappingOperation where T : class
    {
        public PutTypeMappingOperation() : base(typeof(T).Name.ToLowerInvariant()) {}
    }
}
