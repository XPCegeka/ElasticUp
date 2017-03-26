using System;
using Newtonsoft.Json.Linq;

namespace ElasticUp.Operation.Reindex
{
    [Obsolete("Use BatchUpdateOperation")]
    public class BatchUpdateFromTypeToTypeUsingJObjectOperation : BatchUpdateFromTypeToTypeOperation<JObject, JObject>
    {
        public BatchUpdateFromTypeToTypeUsingJObjectOperation(string sourceType, string targetType)
        {
            if (string.IsNullOrWhiteSpace(sourceType)) throw new ArgumentNullException(nameof(sourceType), "Source type should not be null");
            if (string.IsNullOrWhiteSpace(targetType)) throw new ArgumentNullException(nameof(targetType), "Target type should not be null");
            SourceType = sourceType.ToLowerInvariant();
            TargetType = targetType.ToLowerInvariant();
            
        }
    }
}