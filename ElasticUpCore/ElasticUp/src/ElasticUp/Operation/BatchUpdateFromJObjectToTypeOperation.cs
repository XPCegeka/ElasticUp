using System;
using Newtonsoft.Json.Linq;

namespace ElasticUp.Operation
{
    public class BatchUpdateFromJObjectToTypeOperation : BatchUpdateFromTypeToTypeOperation<JObject, JObject>
    {
        public BatchUpdateFromJObjectToTypeOperation(string sourceType, string targetType)
        {
            if (string.IsNullOrWhiteSpace(sourceType)) throw new ArgumentNullException(nameof(sourceType), "Source type should not be null");
            if (string.IsNullOrWhiteSpace(targetType)) throw new ArgumentNullException(nameof(targetType), "Target type should not be null");
            SourceType = sourceType.ToLowerInvariant();
            TargetType = targetType.ToLowerInvariant();
        }
    }
}