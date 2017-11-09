using System;
using Nest;

namespace ElasticUp.Helper
{
    public class IndexSettingsForBulkHelper : IDisposable
    {
        public static readonly Time DefaultRefreshInterval = new Time(TimeSpan.FromSeconds(1));
        public static readonly int DefaultNumberOfReplicas = 1;

        private readonly IElasticClient _elasticClient;
        private readonly bool _enabled = true;

        private readonly string _index;
        private Time _refreshInterval;
        private int? _numberOfReplicas;

        public IndexSettingsForBulkHelper(IElasticClient elasticClient, string index, bool enabled = true)
        {
            _elasticClient = elasticClient;
            _index = index;
            _enabled = enabled;

            PrepareForBulkIndex();
        }

        private void PrepareForBulkIndex()
        {
            if (_enabled) { 
                var settings = _elasticClient.GetIndexSettings(s => s.Index(_index)).Indices[_index].Settings;
                _refreshInterval = settings.RefreshInterval;
                _numberOfReplicas = settings.NumberOfReplicas;
                _elasticClient.UpdateIndexSettings(_index, s => s.IndexSettings(p => p.NumberOfReplicas(0).RefreshInterval(new Time(-1))));
            }
        }

        public void Dispose()
        {
            if (_enabled)
            {
                _elasticClient
                    .UpdateIndexSettings(_index, s => s
                        .IndexSettings(p => p
                            .NumberOfReplicas(_numberOfReplicas ?? DefaultNumberOfReplicas)
                            .RefreshInterval(_refreshInterval ?? DefaultRefreshInterval)));
            }
        }
    }
}
