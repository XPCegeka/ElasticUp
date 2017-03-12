using System;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Validator
{
    public class OperationValidator
    {
        private readonly Type _type;
        private readonly IElasticClient _elasticClient;

        public static OperationValidator ValidatorFor<T>(IElasticClient elasticClient) where T : class
        {
            return new OperationValidator(elasticClient, typeof(T));
        }

        private OperationValidator(IElasticClient elasticClient, Type type)
        {
            _type = type;
            _elasticClient = elasticClient;
        }

        public void IsNotBlank(string value, string message)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new ElasticUpException(ExceptionMessage(message));
        }

        public void IndexExists(string index)
        {
            if (!_elasticClient.IndexExists(index).Exists)
                throw new ElasticUpException(ExceptionMessage($"Index '{index}' does not exist"));
        }

        private string ExceptionMessage(string message) => $"{_type.Name}: {message}";

        public static string RequiredMessage(string fieldName) => $"Field {fieldName} must not be null or empty'";
    }
}