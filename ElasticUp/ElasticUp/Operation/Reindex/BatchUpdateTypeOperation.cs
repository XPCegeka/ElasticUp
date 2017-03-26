using System;

namespace ElasticUp.Operation.Reindex
{
    [Obsolete("Use BatchUpdateOperation")]
    public class BatchUpdateTypeOperation<T> : BatchUpdateFromTypeToTypeOperation<T, T> where T : class {}
}