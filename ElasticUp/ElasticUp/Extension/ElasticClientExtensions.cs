using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Extension
{
    internal static class ElasticClientExtensions
    {
        public static void SetIndexBlocksReadOnly(this IElasticClient elasticClient, string indexName, bool readOnly)
        {
            if (string.IsNullOrEmpty(indexName))
                throw new ElasticUpException($"{nameof(indexName)} cannot be null or empty.", new ArgumentNullException(nameof(indexName)));

            elasticClient.UpdateIndexSettings(indexName,
                descriptor => descriptor
                    .IndexSettings(settingsDescriptor => settingsDescriptor
                        .BlocksReadOnly(readOnly)));
        }

        public static ReadOnlyIndexContext WithReadOnlyIndex(this IElasticClient elasticClient, string indexName)
        {
            if (string.IsNullOrEmpty(indexName))
                throw new ElasticUpException($"{nameof(indexName)} cannot be null or empty.", new ArgumentNullException(nameof(indexName)));

            return new ReadOnlyIndexContext(elasticClient, indexName);
        }

        public static async Task DoScrollAsync<TDocument>(this IElasticClient elasticClient, Func<SearchDescriptor<TDocument>, ISearchRequest> searchDescriptor,
            Action<IEnumerable<TDocument>> onBatchLoaded, int scrollSize = 5000, Time scrollTimeout = null)
            where TDocument : class
        {
            scrollTimeout = scrollTimeout ?? new Time(TimeSpan.FromSeconds(60));
            var searchResponse = elasticClient.Search<TDocument>(descriptor => searchDescriptor(descriptor.Scroll(scrollTimeout).Size(scrollSize)));
            if (searchResponse.Documents.Any())
                onBatchLoaded.Invoke(searchResponse.Documents);

            await DoScrollAsync(elasticClient, scrollTimeout, searchResponse.ScrollId, onBatchLoaded);
        }

        private static async Task DoScrollAsync<TDocument>(IElasticClient elasticClient, Time scrollTimeout, string scrollId, Action<IEnumerable<TDocument>> onBatchLoaded) where TDocument : class
        {
            Func<IEnumerable<TDocument>, Task> asyncOnBatchLoaded = documents => { return Task.Run(() => onBatchLoaded(documents)); };
            var processingTasks = new List<Task>();

            var response = await elasticClient.ScrollAsync<TDocument>(scrollTimeout, scrollId);
            while (response.Documents.Any())
            {
                var responseTask = elasticClient.ScrollAsync<TDocument>(scrollTimeout, response.ScrollId);
                //onBatchLoaded.Invoke(response.Documents);
                processingTasks.Add(asyncOnBatchLoaded.Invoke(response.Documents));
                response = await responseTask;
            }

            await Task.WhenAll(processingTasks);
        }

        public static void DoScroll<TDocument>(this IElasticClient elasticClient, Func<SearchDescriptor<TDocument>, ISearchRequest> searchDescriptor,
           Action<IEnumerable<TDocument>> onBatchLoaded, int scrollSize = 5000, Time scrollTimeout = null)
           where TDocument : class
        {
            scrollTimeout = scrollTimeout ?? new Time(TimeSpan.FromSeconds(60));
            var searchResponse = elasticClient.Search<TDocument>(descriptor => searchDescriptor(descriptor.Scroll(scrollTimeout).Size(scrollSize)));
            if (searchResponse.Documents.Any())
                onBatchLoaded.Invoke(searchResponse.Documents);

            DoScroll(elasticClient, scrollTimeout, searchResponse.ScrollId, onBatchLoaded);
        }

        private static void DoScroll<TDocument>(IElasticClient elasticClient, Time scrollTimeout, string scrollId, Action<IEnumerable<TDocument>> onBatchLoaded) where TDocument : class
        {
            var response = elasticClient.Scroll<TDocument>(scrollTimeout, scrollId);

            while (response.Documents.Any())
            {
                onBatchLoaded.Invoke(response.Documents);
                response = elasticClient.Scroll<TDocument>(scrollTimeout, response.ScrollId);
            }
        }
    }
}