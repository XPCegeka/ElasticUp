using System.IO;

namespace ElasticUp.Tests.Infrastructure
{
    public static class ResourceUtilities
    {

        public static string FromResourceFileToString(string fileName)
        {
            var assembly = typeof(ResourceUtilities).Assembly;
            var resourcePath = assembly.GetName().Name + ".Resources." + fileName;
            var resourceStream = assembly.GetManifestResourceStream(resourcePath);

            using (var stream = resourceStream)
            using (var streamReader = new StreamReader(stream))
            {
                return streamReader.ReadToEnd();
            }
        }

    }
}
