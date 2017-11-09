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
        public int DegreeOfParallellism { get; set; } = 1;
        public string FromIndexName { get; set; }
        public string FromTypeName { get; set; }
        public string ToIndexName { get; set; }
        public string ToTypeName { get; set; }
        public bool IncrementVersionInSameIndex { get; set; }
        public bool UseEfficientIndexSettingsForBulkIndexing { get; set; } = true;
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

        /// <summary>
        /// If your FromIndexName and ToIndexName are the same and you are using ElasticSearch's version
        /// for ConcurrentModification detection, you can use this settings to have BatchUpdateOperation increment the version
        /// 
        /// Note: it is not encouraged to read from and write to the same index
        /// Best practice is to run a BatchUpdateOperation from vX to vY
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> UsingSameIndexAndIncrementingVersion(string indexName)
        {
            BatchUpdateArguments.FromIndexName = indexName?.ToLowerInvariant();
            BatchUpdateArguments.ToIndexName = indexName?.ToLowerInvariant();
            BatchUpdateArguments.IncrementVersionInSameIndex = true;
            return this;
        }

        /// <summary>
        /// Changes the index settings for improved speed while bulk indexing
        /// refresh_interval = -1, number_of_replicas = 0
        /// The index settings will be reset to their original value after finishing the batch update
        /// Default value = true
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> UseEfficientIndexSettingsForBulkIndexing(bool enable)
        {
            BatchUpdateArguments.UseEfficientIndexSettingsForBulkIndexing = enable;
            return this;
        }

        /// <summary>
        /// The index where to get the documents to be tranformed.
        /// This index will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromIndex(string fromIndexName)
        {
            BatchUpdateArguments.FromIndexName = fromIndexName?.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The index where to get the documents to be tranformed.
        /// This index will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromIndex<TFromIndexClass>()
            where TFromIndexClass : class
        {
            BatchUpdateArguments.FromIndexName = typeof(TFromIndexClass).Name.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The index where the transformed documents will be indexed
        /// This index will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToIndex(string toIndexName)
        {
            BatchUpdateArguments.ToIndexName = toIndexName?.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The index where the transformed documents will be indexed
        /// This index will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToIndex<TToIndexClass>() where TToIndexClass : class
        {
            BatchUpdateArguments.ToIndexName = typeof(TToIndexClass).Name.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where to get the documents to be tranformed.
        /// This type will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromType(string fromTypeName)
        {
            BatchUpdateArguments.FromTypeName = fromTypeName?.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where to get the documents to be tranformed.
        /// This type will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> FromType<TFromTypeName>() where TFromTypeName : class
        {
            BatchUpdateArguments.FromTypeName = typeof(TFromTypeName).Name.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where the transformed documents will be indexed
        /// This type will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToType(string toTypeName)
        {
            BatchUpdateArguments.ToTypeName = toTypeName?.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where the transformed documents will be indexed
        /// This type will be used in the SearchDescriptor
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ToType<TToTypeName>() where TToTypeName : class
        {
            BatchUpdateArguments.ToTypeName = typeof(TToTypeName).Name.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where to get the documents to be tranformed. This type will be used in the SearchDescriptor.
        /// The same type will be used to reindex the documents.
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Type(string typeName)
        {
            BatchUpdateArguments.FromTypeName = typeName?.ToLowerInvariant();
            BatchUpdateArguments.ToTypeName = typeName?.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The type where to get the documents to be tranformed. This type will be used in the SearchDescriptor.
        /// The same type will be used to reindex the documents.
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Type<TType>() where TType : class
        {
            BatchUpdateArguments.FromTypeName = typeof(TType).Name.ToLowerInvariant();
            BatchUpdateArguments.ToTypeName = typeof(TType).Name.ToLowerInvariant();
            return this;
        }

        /// <summary>
        /// The size used in the SearchDescriptor 
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> BatchSize(int batchSize = 5000)
        {
            if (batchSize <= 0) throw new ArgumentException($"{nameof(batchSize)} cannot be negative or zero");
            BatchUpdateArguments.BatchSize = batchSize;
            return this;
        }

        /// <summary>
        /// The number of threads that will transform and reindex your documents
        /// This can easily be set to Environment.ProcessorCount
        /// Default value = 1
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> DegreeOfParallellism(int degreeOfParallellism = 3)
        {
            if (degreeOfParallellism <= 0) throw new ArgumentException($"{nameof(degreeOfParallellism)} cannot be negative or zero");
            BatchUpdateArguments.DegreeOfParallellism = degreeOfParallellism;
            return this;
        }

        /// <summary>
        /// The scroll timeout in seconds used in the SearchDescriptor
        /// If your transformation takes a long time it may be necessary to increase the scroll timeout
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> ScrollTimeoutInSeconds(int scrollTimeoutInSeconds = 360)
        {
            if (scrollTimeoutInSeconds <= 0) throw new ArgumentException($"{nameof(scrollTimeoutInSeconds)} cannot be negative or zero");
            BatchUpdateArguments.ScrollTimeoutInSeconds = scrollTimeoutInSeconds;
            return this;
        }

        /// <summary>
        /// The SearchDescriptor to search the documents you want to reindex using the BatchUpdateOperation
        /// ElasticUp will automatically make this a scrolling search
        /// Note: query size can be set using the BatchSize parameter
        /// Note: scroll timeout can be set using the ScrollTimeoutInSeconds parameter
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> SearchDescriptor(Func<SearchDescriptor<TTransformFrom>, ISearchRequest> searchDescriptor)
        {
            if (searchDescriptor == null) throw new ArgumentNullException(nameof(searchDescriptor));
            BatchUpdateArguments.SearchDescriptor = searchDescriptor;
            return this;
        }

        /// <summary>
        /// The transformation to transform your documents before reindexing.
        /// Add your custom code here.
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> Transformation(Func<TTransformFrom, TTransformTo> transformation)
        {
            if (transformation == null) throw new ArgumentNullException(nameof(transformation));
            BatchUpdateArguments.Transformation = transformation;
            return this;
        }

        /// <summary>
        /// A hook to execute custom code after each document that was transformed and reindexed.
        /// </summary>
        public BatchUpdateDescriptor<TTransformFrom, TTransformTo> OnDocumentProcessed(Action<TTransformTo> onDocumentProcessed)
        {
            if (onDocumentProcessed == null) throw new ArgumentNullException(nameof(onDocumentProcessed));
            BatchUpdateArguments.OnDocumentProcessed = onDocumentProcessed;
            return this;
        }

        public static implicit operator BatchUpdateArguments<TTransformFrom, TTransformTo>(BatchUpdateDescriptor<TTransformFrom, TTransformTo> descriptor)
        {
            return descriptor.BatchUpdateArguments;
        }
    }
}