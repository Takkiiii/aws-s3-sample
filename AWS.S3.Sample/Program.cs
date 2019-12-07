using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace AWS.S3.Sample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var awsAccessKeyId = "xxxxxxxxxxxxxxx";
            var awsSecretAccessKey = "xxxxxxxxxxxxxxx";
            var srcPath = "xxxxxxxxxxxxxxx";
            var src = new FileInfo(srcPath);
            var bucket = "xxxxxxxxxxxxxxx";
            var key = "xxxxxxxxxxxxxxx";
            const int partSize = 5 << 20;
            using (var client = new S3Client(awsAccessKeyId, awsSecretAccessKey, RegionEndpoint.APNortheast1))
            {
                await client.UploadAsync(src, bucket, key, partSize);
            }
        }
    }

    public class S3Client : IDisposable
    {
        private readonly AmazonS3Client client;

        public S3Client(string awsAccessKeyId, string awsSecretAccessKey, RegionEndpoint regionEndpoint)
        {
            var amazonS3Config = new AmazonS3Config
            {
                RegionEndpoint = regionEndpoint
            };
            client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, amazonS3Config);
        }

        public async Task UploadAsync(System.IO.FileInfo src, string bucket, string key, int partSize, CancellationToken token = default)
        {
            var uploadResponses = new List<UploadPartResponse>();

            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = bucket,
                Key = key
            };

            var initResponse =
                await client.InitiateMultipartUploadAsync(initiateRequest, token);

            var contentLength = src.Length;

            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = initResponse.UploadId
            };
            try
            {
                int filePosition = 0;
                for (var i = 1; filePosition < contentLength; i++)
                {
                    var md5 = Calculate(src.FullName, filePosition, partSize);
                    var uploadRequest = new UploadPartRequest
                    {
                        BucketName = bucket,
                        Key = key,
                        UploadId = initResponse.UploadId,
                        PartNumber = i,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        FilePath = src.FullName,
                        MD5Digest = Convert.ToBase64String(md5)
                    };

                    var ret = await client.UploadPartAsync(uploadRequest, token);
                    uploadResponses.Add(ret);
                    filePosition += partSize;
                }

                completeRequest.AddPartETags(uploadResponses);
            }
            catch (Exception)
            {
                var abortMpuRequest = new AbortMultipartUploadRequest
                {
                    BucketName = bucket,
                    Key = key,
                    UploadId = initResponse.UploadId
                };
                await client.AbortMultipartUploadAsync(abortMpuRequest, token);
                throw;
            }
        }


        public byte[] Calculate(string fn, long position, int count)
        {
            using (var s = File.OpenRead(fn))
            {
                s.Position = position;
                return Calculate(s, count);
            }
        }

        public byte[] Calculate(Stream stream, int count)
        {
            var offset = 0;
            var bufferSize = 4096 > count ? count : 4096;
            var buffer = new byte[bufferSize];

            using (var e = MD5.Create())
            {
                while (offset < count)
                {
                    var chunkSize = count >= bufferSize ? bufferSize : count;
                    var read = stream.Read(buffer, 0, chunkSize);
                    if (read == 0)
                    {
                        break;
                    }

                    offset += read;
                    e.TransformBlock(buffer, 0, read, null, 0);
                }

                e.TransformFinalBlock(buffer, 0, 0);
                return e.Hash;
            }
        }

        public void Dispose()
        {
            client?.Dispose();
        }
    }
}
