using System.IO;
using System.Text;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Quartz.Spi.CosmosDbJobStore.Util
{
    /// <summary>
    /// Current CosmosDB driver don't let us configure serialization of inherited classes
    /// </summary>
    public class QuartzCosmosSerializer : CosmosSerializer
    {
        private static readonly Encoding DefaultEncoding = new UTF8Encoding(false, true);
        private readonly JsonSerializer _serializer;


        public QuartzCosmosSerializer()
        {
            _serializer = JsonSerializer.Create(new InheritedJsonObjectSerializer().CreateSerializerSettings());
        }

        
        public override T FromStream<T>(Stream stream)
        {
            using (stream)
            {
                using (var streamReader = new StreamReader(stream))
                {
                    using (var jsonTextReader = new JsonTextReader(streamReader))
                    {
                        return _serializer.Deserialize<T>(jsonTextReader);
                    }
                }
            }
        }

        public override Stream ToStream<T>(T input)
        {
            var memoryStream = new MemoryStream();
            using (var streamWriter = new StreamWriter(memoryStream, DefaultEncoding, 1024, true))
            {
                using (JsonWriter jsonWriter = new JsonTextWriter(streamWriter))
                {
                    jsonWriter.Formatting = Formatting.None;
                    _serializer.Serialize(jsonWriter, input);
                    jsonWriter.Flush();
                    streamWriter.Flush();
                }
            }

            memoryStream.Position = 0L;
            return memoryStream;
        }
    }
}