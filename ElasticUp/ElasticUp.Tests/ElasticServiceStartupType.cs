namespace ElasticUp.Tests
{
    public enum ElasticServiceStartupType
    {
        /// <summary>
        /// Do not start the ElasticSearch service
        /// </summary>
        NoStartup,

        /// <summary>
        /// Start the ElasticSearch service once, and reuse it between tests
        /// </summary>
        OneTimeStartup,

        /// <summary>
        /// Start a new ElasticSearch service for each test and stop the service after the test
        /// </summary>
        StartupForEach
    }
}