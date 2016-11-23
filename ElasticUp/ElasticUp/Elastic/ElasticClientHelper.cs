using System;
using Nest;

namespace ElasticUp.Elastic
{
    public static class ElasticClientHelper
    {
        public static T ValidateElasticResponse<T>(T getResponse) where T : IResponse
        {
            if (!getResponse.IsValid)
            {
                var elasticUpException = new ElasticUpException($"Exception when calling elasticsearch", getResponse?.ServerError, getResponse?.DebugInformation);
                Console.Write(elasticUpException);
                throw elasticUpException;
            }
            return getResponse;
        }

    }
}
