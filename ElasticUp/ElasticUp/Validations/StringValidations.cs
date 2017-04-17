using System;
using ElasticUp.Util;

namespace ElasticUp.Validations
{
    public class StringValidations
    {
        private readonly Type _type;

        public static string RequiredMessage(string fieldName) => $"Field {fieldName} must not be null or empty'";

        private string ExceptionMessage(string message) => $"{_type.Name}: {message}";

        private StringValidations(Type type)
        {
            _type = type;
        }

        public static StringValidations StringValidationsFor<T>() where T : class
        {
            return new StringValidations(typeof(T));
        }

        public StringValidations IsNotBlank(string value, string message = "Value is null or empty")
        {
            if (string.IsNullOrWhiteSpace(value)) throw new ElasticUpException(ExceptionMessage(message));
            return this;
        }

        public StringValidations IsBlank(string value, string message = "Value is not null or empty")
        {
            if (!string.IsNullOrWhiteSpace(value)) throw new ElasticUpException(ExceptionMessage(message));
            return this;
        }
    }
}