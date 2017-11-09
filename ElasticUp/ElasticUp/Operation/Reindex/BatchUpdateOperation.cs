using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using ElasticUp.Helper;
using ElasticUp.Util;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Reindex
{

    /// <summary>
    /// This operation will 
    /// - read documents from a given index (using an elasticsearch SearchDescriptor)
    /// - transform the documents using your function
    /// - reindex the documents to a given index (using bulk index)
    /// 
    /// The transformation and reindexing will happen in multiple threads according to 
    /// 
    /// </summary>
    /// <typeparam name="TTransformFromType"></typeparam>
    /// <typeparam name="TTransformToType"></typeparam>
    public class BatchUpdateOperation<TTransformFromType, TTransformToType> : AbstractElasticUpOperation
                        where TTransformFromType : class
                        where TTransformToType : class
    {
        private readonly BatchUpdateArguments<TTransformFromType, TTransformToType> _arguments;

        public BatchUpdateOperation(Func<BatchUpdateDescriptor<TTransformFromType, TTransformToType>, BatchUpdateArguments<TTransformFromType, TTransformToType>> arguments)
        {
            _arguments = arguments.Invoke(new BatchUpdateDescriptor<TTransformFromType, TTransformToType>());
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<BatchUpdateOperation<TTransformFromType, TTransformToType>>()
                .IsNotBlank(_arguments.FromIndexName, RequiredMessage("FromIndexName"))
                .IsNotBlank(_arguments.ToIndexName, RequiredMessage("ToIndexName"))
                .IsNotBlank(_arguments.FromTypeName, RequiredMessage("FromTypeName"))
                .IsNotBlank(_arguments.ToTypeName, RequiredMessage("ToTypeName"));

            IndexValidationsFor<BatchUpdateOperation<TTransformFromType, TTransformToType>>(elasticClient)
                .IndexExists(_arguments.FromIndexName)
                .IndexExists(_arguments.ToIndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            using (new IndexSettingsForBulkHelper(elasticClient, _arguments.ToIndexName, _arguments.UseEfficientIndexSettingsForBulkIndexing))
            {
                var cancellationToken = new CancellationTokenSource();
                var collectionQueue = new BlockingCollection<ISearchResponse<TTransformFromType>>(new ConcurrentQueue<ISearchResponse<TTransformFromType>>(), _arguments.DegreeOfParallellism * 10);
                
                var tasks = new List<Task>();

                var producer = new TaskProducer<TTransformFromType, TTransformToType>(collectionQueue, elasticClient, _arguments, cancellationToken);
                var producerTask = Task.Run(() => producer.ProduceTasks());
                tasks.Add(producerTask);

                for (var i = 0; i < _arguments.DegreeOfParallellism; i++)
                {
                    var consumer = new TaskConsumer<TTransformFromType, TTransformToType>(collectionQueue, elasticClient, _arguments, cancellationToken);
                    var consumerTask = Task.Run(() => consumer.ConsumeTasks());
                    tasks.Add(consumerTask);
                }
                
                Task.WaitAll(tasks.ToArray());
            }
        }
    }

    public class TaskProducer<TTransformFromType, TTransformToType>
        where TTransformFromType : class
        where TTransformToType : class
    {
        private readonly IElasticClient _elasticClient;
        private readonly BlockingCollection<ISearchResponse<TTransformFromType>> _taskQueue;
        private readonly BatchUpdateArguments<TTransformFromType, TTransformToType> _arguments;
        private readonly CancellationTokenSource _cts;

        public TaskProducer(BlockingCollection<ISearchResponse<TTransformFromType>> taskQueue, IElasticClient elasticClient,  BatchUpdateArguments<TTransformFromType, TTransformToType> arguments, CancellationTokenSource cts)
        {
            _elasticClient = elasticClient;
            _arguments = arguments;
            _cts = cts;
            _taskQueue = taskQueue;
        }

        public void ProduceTasks()
        {
            var searchResponse = Search(_elasticClient);
            if (!searchResponse.Documents.Any())
            {
                _taskQueue.CompleteAdding();
                return;
            }
            Console.WriteLine($"Need to process {searchResponse.Total} items");

            while (searchResponse.Documents.Any() && !_cts.IsCancellationRequested)
            {
                _taskQueue.TryAdd(searchResponse, Int32.MaxValue);
                searchResponse = _elasticClient.Scroll<TTransformFromType>(_arguments.ScrollTimeout, searchResponse.ScrollId);
            }
            _taskQueue.CompleteAdding();
        }

        protected ISearchResponse<TTransformFromType> Search(IElasticClient elasticClient)
        {
            return elasticClient
                .Search<TTransformFromType>(descriptor => _arguments.SearchDescriptor(descriptor
                    .Index(_arguments.FromIndexName)
                    .Type(_arguments.FromTypeName)
                    .Version()
                    .Scroll(_arguments.ScrollTimeout)
                    .Size(_arguments.BatchSize)));
        }
    }

    public class TaskConsumer<TTransformFromType, TTransformToType>
        where TTransformFromType : class
        where TTransformToType : class
    {
        private readonly IElasticClient _elasticClient;
        private BlockingCollection<ISearchResponse<TTransformFromType>> _taskQueue;
        private readonly BatchUpdateArguments<TTransformFromType, TTransformToType> _arguments;
        private readonly CancellationTokenSource _cts;

        public TaskConsumer(BlockingCollection<ISearchResponse<TTransformFromType>> taskQueue, IElasticClient elasticClient, BatchUpdateArguments<TTransformFromType, TTransformToType> arguments, CancellationTokenSource cts)
        {
            _elasticClient = elasticClient;
            _arguments = arguments;
            _cts = cts;
            _taskQueue = taskQueue;
        }

        public void ConsumeTasks()
        {

            while (!_taskQueue.IsCompleted && !_cts.IsCancellationRequested)
            {
                ISearchResponse<TTransformFromType> response;
                bool success = _taskQueue.TryTake(out response, Int32.MaxValue);
                if (success)
                {
                    try
                    {
                        ProcessHits(_elasticClient, response.Hits);
                    }
                    catch (Exception)
                    {
                        _cts.Cancel();
                        throw;
                    }
                }
            }
        }

        protected virtual void ProcessHits(IElasticClient elasticClient, IEnumerable<IHit<TTransformFromType>> hits)
        {
            var transformedDocuments = TransformDocuments(hits).ToList();
            
            BulkIndex(elasticClient, transformedDocuments);
            
            if (_arguments.OnDocumentProcessed != null)
            {
                transformedDocuments.ForEach(doc => _arguments.OnDocumentProcessed?.Invoke(doc.TransformedHit));
            }
        }

        protected void BulkIndex(IElasticClient elasticClient, IEnumerable<TransformedDocument<TTransformFromType, TTransformToType>> transformedDocuments)
        {
            var bulkDescriptor = new BulkDescriptor();

            foreach (var document in transformedDocuments)
            {
                if (document.TransformedHit == null)
                    continue;

                if (document.Hit.Version.HasValue)
                {
                    var version = document.Hit.Version;
                    if (_arguments.IncrementVersionInSameIndex)
                    {
                        version++;
                    }
                    bulkDescriptor.Index<object>(
                        descr => descr
                            .Index(_arguments.ToIndexName)
                            .Id(document.Hit.Id)
                            .Type(_arguments.ToTypeName)
                            .VersionType(VersionType.External)
                            .Version(version)
                            .Document(document.TransformedHit));
                }
                else
                {
                    bulkDescriptor.Index<object>(
                        descr => descr
                            .Index(_arguments.ToIndexName)
                            .Id(document.Hit.Id)
                            .Type(_arguments.ToTypeName)
                            .Document(document.TransformedHit));
                }
            }


            var bulkResponse = elasticClient.Bulk(bulkDescriptor);
            if (!bulkResponse.IsValid)
            {
                throw new ElasticUpException("BatchUpdateOperation: failed to bulkIndex models:" + bulkResponse.DebugInformation);
            }
        }

        protected IEnumerable<TransformedDocument<TTransformFromType, TTransformToType>> TransformDocuments(IEnumerable<IHit<TTransformFromType>> hits)
        {
            return hits
                .Where(hit => hit.Source != null)
                .Select(hit => new TransformedDocument<TTransformFromType, TTransformToType>
                {
                    Hit = hit,
                    TransformedHit = _arguments.Transformation(hit.Source)
                });
        }
    }
}