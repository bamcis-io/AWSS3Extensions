using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using BAMCIS.AWSLambda.Common;
using BAMCIS.AWSS3FastCopy.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace BAMCIS.AWSS3FastCopy.Tests
{
    public class AmazonS3ExtensionMethodsTests
    {
        private static AWSCredentials creds;
        private static string sourceBucket;
        private static string destinationBucket;
        private static IAmazonS3 client;

        private static int MIN_SIZE_128KB = 128 * 1024;
        private static int MAX_SIZE_10MB = 10 * 1024 * 1024;

        static AmazonS3ExtensionMethodsTests()
        {
            AWSConfigs.AWSProfilesLocation = $"{Environment.GetEnvironmentVariable("UserProfile")}\\.aws\\credentials";
            SharedCredentialsFile file = new SharedCredentialsFile();
            file.TryGetProfile($"{Environment.UserName}-dev", out CredentialProfile profile);
            creds = profile.GetAWSCredentials(file);
            sourceBucket = $"{Environment.UserName}-{Guid.NewGuid().ToString()}";
            destinationBucket = sourceBucket;
            client = new AmazonS3Client(creds);
        }


        [Fact]
        public void TestConvertRequest()
        {
            // ARRANGE
            CopyObjectRequest copyRequest = new CopyObjectRequest()
            {
                DestinationBucket = "destination-bucket",
                SourceBucket = "source-bucket",
                SourceKey = "source/key/test.txt",
                DestinationKey = "destination/key/test.txt",
                TagSet = new List<Tag>() { new Tag() { Key = "tag1", Value = "myval" } }
            };

            MethodInfo dynMethod = typeof(AmazonS3ExtensionMethods).GetMethod("ConvertTo", BindingFlags.NonPublic | BindingFlags.Static);

            MethodInfo genericMethod = dynMethod.MakeGenericMethod(typeof(CopyPartRequest));

            // ACT
            CopyPartRequest Request = (CopyPartRequest)genericMethod.Invoke(null, new object[] { copyRequest });

            // ASSERT
            Assert.NotNull(Request);
            Assert.Equal(copyRequest.DestinationBucket, Request.DestinationBucket);
            Assert.Equal(copyRequest.SourceBucket, Request.SourceBucket);
            Assert.Equal(copyRequest.DestinationKey, Request.DestinationKey);
            Assert.Equal(copyRequest.SourceKey, Request.SourceKey);
            Assert.Equal(copyRequest.SourceVersionId, Request.SourceVersionId);
        }

        [Fact]
        public async Task CopyObjectTest()
        {
            // ARRANGE
            CopyObjectRequest request = new CopyObjectRequest()
            {
                DestinationBucket = "mhaken",
                SourceBucket = "mhaken-lambda",
                SourceKey = "AWSAthenaUserMetrics/athena-metrics-636765132762278062.zip",
                DestinationKey = "test/file.txt",
            };

            GetObjectMetadataRequest meta = new GetObjectMetadataRequest()
            {
                BucketName = request.SourceBucket,
                Key = request.SourceKey
            };

            GetObjectMetadataResponse Meta = await client.GetObjectMetadataAsync(meta);

            // ACT
            CopyObjectResponse Response = await client.CopyOrMoveObjectAsync(request, 16777216, false);


            // ASSERT
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
            Assert.Equal(Meta.ETag, Response.ETag);
        }

        [Fact]
        public async Task MoveObjectTest()
        {
            // ARRANGE
            CopyObjectRequest req = new CopyObjectRequest()
            {
                DestinationBucket = destinationBucket,
                SourceBucket = sourceBucket,
                SourceKey = "test/file.txt",
                DestinationKey = "test/file2.txt",
            };

            GetObjectMetadataRequest metaReq = new GetObjectMetadataRequest()
            {
                BucketName = req.SourceBucket,
                Key = req.SourceKey
            };

            GetObjectMetadataResponse meta = await client.GetObjectMetadataAsync(metaReq);

            // ACT
            CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req, 16777216, true);

            // ASSERT
            Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
            Assert.Equal(meta.ETag, response.ETag);
        }

        [Fact]
        public async Task BatchMoveItems()
        {
            // ARRANGE
            string bucket = sourceBucket;
            //string bucket = "mhaken-9fc61dff-1893-42cc-a82f-8024960b158e";

            Stopwatch sw = new Stopwatch();
            int count = 10000;

            try
            {
                sw.Start();

                await CreateAndFillBucket(count);

                sw.Stop();

                Debug.Print($"Finished creating and filling bucket {bucket} in {sw.Elapsed}.");

                sw.Reset();

                ListObjectsV2Request list = new ListObjectsV2Request()
                {
                    BucketName = bucket
                };

                sw.Start();

                IEnumerable<S3Object> objects = (await client.ListAllObjectsAsync(list)).SelectMany(x => x.S3Objects);

                // ACT
                BatchMoveRequest request = new BatchMoveRequest(objects.Select(x => new CopyObjectRequest()
                {
                    SourceBucket = x.BucketName,
                    DestinationBucket = x.BucketName,
                    SourceKey = x.Key,
                    DestinationKey = "moved_" + x.Key,
                    StorageClass = S3StorageClass.OneZoneInfrequentAccess,
                }))
                {
                    PreferMultipart = true
                };


                BatchCopyResponse response = await client.BatchMoveAsync(request);

                sw.Stop();

                File.WriteAllText("results.txt", $"Successfully moved {count} items in {sw.Elapsed}.");

                // ASSERT
                Assert.Equal(objects.Count(), response.SuccessfulResponses.Count);
            }
            catch (Exception e)
            {

            }
            finally
            {
                await client.EmptyBucket(bucket);

                await client.DeleteBucketAsync(bucket);
            }
        }

        private async Task CreateAndFillBucket(int numberOfObjects)
        {
            bool exists = await client.BucketExists(sourceBucket);

            List<PutObjectResponse> fails = new List<PutObjectResponse>();

            if (!exists)
            {
                PutBucketRequest newBucket = new PutBucketRequest()
                {
                    BucketName = sourceBucket,
                    BucketRegion = S3Region.US
                };

                PutBucketResponse response = await client.PutBucketAsync(newBucket);
            }

            Random rand = new Random();

            List<Task<PutObjectResponse>> tasks = new List<Task<PutObjectResponse>>();

            int totalJobs = numberOfObjects / 100;

            if (totalJobs == 0)
            {
                totalJobs = 1;
            }

            for (int i = 0; i < totalJobs; i++)
            {
                int files = (i == totalJobs - 1 ? (numberOfObjects % 100 == 0 ? 100 : numberOfObjects % 100) : 100);

                for (int j = 0; j < files; j++)
                {
                    int size = rand.Next(MIN_SIZE_128KB, MAX_SIZE_10MB);

                    byte[] contents = new byte[size];

                    rand.NextBytes(contents);

                    PutObjectRequest put = new PutObjectRequest()
                    {
                        BucketName = sourceBucket,
                        Key = Guid.NewGuid().ToString(),
                        StorageClass = S3StorageClass.OneZoneInfrequentAccess,
                        ContentType = "application/octet-stream",
                        AutoCloseStream = true,
                        TagSet = new List<Tag>() { new Tag() { Key = "owner", Value = Environment.UserName } },
                        InputStream = new MemoryStream(contents)
                    };

                    tasks.Add(client.PutObjectAsync(put));
                }

                try
                {                   
                    int counter = 0;

                    foreach (Task<PutObjectResponse> response in tasks.Interleaved())
                    {
                        counter++;
                        if (response.Result.HttpStatusCode != HttpStatusCode.OK)
                        {
                            fails.Add(response.Result);
                        }
                    }

                    tasks.Clear();
                    tasks = new List<Task<PutObjectResponse>>();

                    GC.Collect();

                    if (counter != files)
                    {

                    }
                }
                catch (Exception e)
                {

                }
            }
        }
    }
}
