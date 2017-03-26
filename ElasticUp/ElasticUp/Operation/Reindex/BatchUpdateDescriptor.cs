using System;
using Nest;

namespace ElasticUp.Operation.Reindex
{

    public class BatchUpdateArguments<TTransformFrom, TTransformTo> where TTransformFrom : class
        where TTransformTo : class
    {
        public Time ScrollTimeout => new Time(TimeSpan.FromSeconds(ScrollTimeoutInSeconds));
        public double ScrollTimeoutInSeconds = 360;
        public int BatchSize { get; set; } = 5000;
        public int DegreeOfParallellism { get; set; } = 5;

        public string FromIndexName { get; set; }
        public string FromTypeName { get; set; }
        public string ToIndexName { get; set; }
        public string ToTypeName { get; set; }

        public Func<SearchDescriptor<TTransformFrom>, ISearchRequest> SearchDescriptor { get; set; } = s => s;
        public Func<TTransformFrom, TTransformTo> Transformation { get; set; } = t => t as TTransformTo;
        public Action<TTransformTo> OnDocumentProcessed { get; set; }
    }

    public class BatchUpdateDescriptor<TTransformFrom, TTransformTo> where TTransformFrom : class
        where TTransformTo : class
    {
        public readonly BatchUpdateArguments<TTransformFrom, TTransformTo> BatchUpdateArguments;

        public BatchUpdateDescriptor()
        {
            BatchUpdateArguments = new BatchUpdateArguments<TTransformFrom, TTransformTo>();
            FromType<TTransformFrom>();
            ToType<TTransformTo>();
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromIndex(string fromIndexName)
        {
            BatchUpdateArguments.FromIndexName = fromIndexName?.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromIndex<TFromIndexClass>()
            where TFromIndexClass : class
        {
            BatchUpdateArguments.FromIndexName = typeof(TFromIndexClass).Name.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToIndex(string toIndexName)
        {
            BatchUpdateArguments.ToIndexName = toIndexName?.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToIndex<TToIndexClass>() where TToIndexClass : class
        {
            BatchUpdateArguments.ToIndexName = typeof(TToIndexClass).Name.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromType(string fromTypeName)
        {
            BatchUpdateArguments.FromTypeName = fromTypeName?.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromType<TFromTypeName>() where TFromTypeName : class
        {
            BatchUpdateArguments.FromTypeName = typeof(TFromTypeName).Name.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToType(string toTypeName)
        {
            BatchUpdateArguments.ToTypeName = toTypeName?.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToType<TToTypeName>() where TToTypeName : class
        {
            BatchUpdateArguments.ToTypeName = typeof(TToTypeName).Name.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Type(string typeName)
        {
            BatchUpdateArguments.FromTypeName = typeName?.ToLowerInvariant();
            BatchUpdateArguments.ToTypeName = typeName?.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Type<TType>() where TType : class
        {
            BatchUpdateArguments.FromTypeName = typeof(TType).Name.ToLowerInvariant();
            BatchUpdateArguments.ToTypeName = typeof(TType).Name.ToLowerInvariant();
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> BatchSize(int batchSize = 5000)
        {
            BatchUpdateArguments.BatchSize = batchSize;
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> DegreeOfParallellism(int degreeOfParallellism = 5)
        {
            BatchUpdateArguments.DegreeOfParallellism = degreeOfParallellism;
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ScrollTimeoutInSeconds(
            int scrollTimeoutInSeconds = 360)
        {
            BatchUpdateArguments.ScrollTimeoutInSeconds = scrollTimeoutInSeconds;
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> SearchDescriptor(
            Func<SearchDescriptor<TTransformFrom>, ISearchRequest> searchDescriptor)
        {
            BatchUpdateArguments.SearchDescriptor = searchDescriptor;
            return this;
        }

        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Transformation(
            Func<TTransformFrom, TTransformTo> transformation)
        {
            BatchUpdateArguments.Transformation = transformation;
            return this;
        }

        public static implicit operator BatchUpdateArguments<TTransformFrom, TTransformTo>(
            BatchUpdateDescriptor<TTransformFrom, TTransformTo> descriptor)
        {

            return descriptor.BatchUpdateArguments;
        }
    }
}