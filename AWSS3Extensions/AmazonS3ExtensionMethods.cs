using Amazon.S3;
using Amazon.S3.Model;
using BAMCIS.AWSS3Extensions.Model;
using BAMCIS.ExponentialBackoffAndRetry;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;

namespace BAMCIS.AWSS3Extensions
{
    /// <summary>
    /// Extension methods for use with the AWS S3 SDK.
    /// </summary>
    public static class AmazonS3ExtensionMethods
    {
        #region Private Fields

        private static Func<long, long, bool> preferMultipartLogic = (objectSizeInBytes, chunkSizeInBytes) => { return objectSizeInBytes >= Constants.MINIMUM_MULTIPART_SIZE && chunkSizeInBytes < objectSizeInBytes; };

        private static Func<long, long, bool> standardMultipartLogic = (objectSize, chunkSizeInBytes) => { return objectSize > Constants.FIVE_GIBIBYTE; };

        #endregion

        #region Public Methods

        /// <summary>
        /// Empties all contents of the specified bucket.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="bucketName">The name of the bucket to empty</param>
        /// <param name="includeAllVersions">If true, in a bucket with versioning turned on, this will delete all versions of
        /// the objects in the bukcet, not just add a delete marker as the most recent version.</param>
        /// <returns></returns>
        public static async Task<IEnumerable<DeleteError>> EmptyBucket(this IAmazonS3 client, string bucketName, bool includeAllVersions = false)
        {
            ParameterTests.NotNullOrEmpty(bucketName, "bucketName");

            DeleteObjectsRequest delete = new DeleteObjectsRequest()
            {
                BucketName = bucketName
            };

            List<DeleteError> errors = new List<DeleteError>();

            if (includeAllVersions)
            {
                ListVersionsResponse response;

                ListVersionsRequest request = new ListVersionsRequest()
                {
                    BucketName = bucketName
                };

                do
                {
                    response = await client.ListVersionsAsync(request);
                    request.KeyMarker = response.NextKeyMarker;
                    request.VersionIdMarker = response.VersionIdMarker;

                    delete.Objects = response.Versions.Select(x => new KeyVersion() { Key = x.Key, VersionId = x.VersionId }).ToList();

                    DeleteObjectsResponse deleteResponse = await client.DeleteObjectsAsync(delete);
                    errors.AddRange(deleteResponse.DeleteErrors);

                } while (response.IsTruncated);
            }
            else
            {
                ListObjectsV2Response response;

                ListObjectsV2Request request = new ListObjectsV2Request()
                {
                    BucketName = bucketName
                };

                do
                {
                    response = await client.ListObjectsV2Async(request);
                    request.ContinuationToken = response.NextContinuationToken;

                    delete.Objects = response.S3Objects.Select(x => new KeyVersion() { Key = x.Key }).ToList();

                    DeleteObjectsResponse deleteResponse = await client.DeleteObjectsAsync(delete);
                    errors.AddRange(deleteResponse.DeleteErrors);

                } while (response.IsTruncated);
            }

            return errors;
        }

        /// <summary>
        /// Tests if an S3 bucket exists by creating a pre-signed url and performing a
        /// HEAD operation on the url. This only tests for buckets that you have access to.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="bucketName">The bucket to test for existence</param>
        /// <returns></returns>
        public static async Task<bool> BucketExists(this IAmazonS3 client, string bucketName)
        {
            ParameterTests.NotNullOrEmpty(bucketName, "bucketName");

            GetPreSignedUrlRequest request = new GetPreSignedUrlRequest()
            {
                BucketName = bucketName,
                Expires = DateTime.Now.AddMinutes(5),
                Verb = HttpVerb.HEAD
            };

            string response = client.GetPreSignedURL(request);

            HttpClient http = new HttpClient();
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Head, response);
            HttpResponseMessage result = await http.SendAsync(message);

            return result.IsSuccessStatusCode;
        }

        /// <summary>
        /// Lists all objects in a bucket
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="bucketName">The bucket name</param>
        /// <param name="prefix">An optional prefix to filter with</param>
        /// <returns></returns>
        public static async Task<IEnumerable<ListObjectsV2Response>> ListAllObjectsAsync(this IAmazonS3 client, string bucketName, string prefix = "")
        {
            ParameterTests.NotNullOrEmpty(bucketName, "bucketName");
            ParameterTests.NonNull<IAmazonS3>(client, "client");

            ListObjectsV2Request request = new ListObjectsV2Request()
            {
                BucketName = bucketName,
                Prefix = prefix ?? String.Empty
            };

            return await ListAllObjectsAsync(client, request);
        }

        /// <summary>
        /// List all objects in a bucket
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="request">The list objects request to perform</param>
        /// <returns></returns>
        public static async Task<IEnumerable<ListObjectsV2Response>> ListAllObjectsAsync(this IAmazonS3 client, ListObjectsV2Request request)
        {
            ParameterTests.NonNull<ListObjectsV2Request>(request, "request");
            ParameterTests.NonNull<IAmazonS3>(client, "client");

            ListObjectsV2Response response;

            List<ListObjectsV2Response> responses = new List<ListObjectsV2Response>();

            request.ContinuationToken = null;

            do
            {
                response = await client.ListObjectsV2Async(request);

                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    responses.Add(response);
                    request.ContinuationToken = response.NextContinuationToken;
                }
                else
                {
                    throw new InvalidOperationException($"The list objects response with id {response.ResponseMetadata.RequestId} provided an http {(int)response.HttpStatusCode} {response.HttpStatusCode} response.");
                }
            } while (response.IsTruncated);

            return responses;
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <param name="partSize">The size of the part to use for a multipart copy.</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchCopyAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests, long partSize, bool preferMultipart = false)
        {
            BatchCopyRequest request = new BatchCopyRequest(requests)
            {
                PartSize = partSize,
                PreferMultipart = preferMultipart
            };

            return await BatchCopyAsync(client, request);
        }

        /// <summary>
        /// Initiates a parallel copy operation for the all of the included requests. Uses a 
        /// 5 MiB part size for multi-part uploads. Does not prefer to use multi-part uploads.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchCopyAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests)
        {
            return await BatchCopyAsync(client, requests, Constants.FIVE_MEBIBYTE);
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests. Uses a
        /// 5 MiB part size for multi-part uploads.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchCopyAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests, bool preferMultipart)
        {
            return await BatchCopyAsync(client, requests, Constants.FIVE_MEBIBYTE, preferMultipart);
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests. Uses a 
        /// 5 MiB part size for multi-part uploads.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="request">The Batch copy request to process</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchCopyAsync(this IAmazonS3 client, BatchCopyRequest request)
        {
            return (await CoreBatchCopyAsync(client, request, false));
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests. The source objects
        /// are either deleted in a batch-type operation or individually as each copy completes.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <param name="partSize">The size of the part to use for a multipart copy.</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <param name="useBatchDelete">If set to true, the objects will be deleted using request batches of 1000 keys per
        /// request. If set to false, each object will be deleted individually after it is copied</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchMoveAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests, long partSize, bool useBatchDelete = true, bool preferMultipart = false)
        {
            BatchMoveRequest request = new BatchMoveRequest(requests)
            {
                PartSize = partSize,
                BatchDelete = useBatchDelete,
                PreferMultipart = preferMultipart
            };

            return await BatchMoveAsync(client, request);
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests. Uses a
        /// 5 MiB part size for multi-part uploads.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <param name="useBatchDelete">If set to true, the objects will be deleted using request batches of 1000 keys per
        /// request. If set to false, each object will be deleted individually after it is copied</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchMoveAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests, bool useBatchDelete, bool preferMultipart = false)
        {
            return await BatchMoveAsync(client, requests, Constants.FIVE_MEBIBYTE, useBatchDelete, preferMultipart);
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests. Uses a
        /// 5 MiB part size for multi-part uploads. Using batch delete is true and prefering
        /// multi-part is set to false.
        /// </summary>
        /// <param name="client">The S3 client to use</param>
        /// <param name="requests">The copy requests to process</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchMoveAsync(this IAmazonS3 client, IEnumerable<CopyObjectRequest> requests)
        {
            return await BatchMoveAsync(client, requests, Constants.FIVE_MEBIBYTE);
        }

        /// <summary>
        /// Initiates a parallel copy operation for all of the included requests.
        /// </summary>
        /// <param name="client">The Amazon S3 Client to use</param>
        /// <param name="request">The batch move request to process</param>
        /// <returns>The copy object responses.</returns>
        public static async Task<BatchCopyResponse> BatchMoveAsync(this IAmazonS3 client, BatchMoveRequest request)
        {
            return await MoveBatchAsnyc(client, request);
        }

        /// <summary>
        /// Copies or moves an S3 object to another location. This method prefers using a multipart copy as long as the specified part size is less
        /// than the source object's size. If the file exists at the destination, if will be overwritten with no warning.
        /// </summary>
        /// <param name="client">The Amazon S3 Client</param>
        /// <param name="request">The Copy Object Request</param>
        /// <param name="partSize">The size of the part to use for a multipart copy.</param>
        /// <param name="deleteSource">If set to true, the source object will be deleted if it was successfully copied. The default is false.</param>
        /// <returns>The copy object response</returns>
        public static async Task<CopyObjectResponse> CopyOrMoveObjectMultipartAsync(this IAmazonS3 client, CopyObjectRequest request, long partSize, bool deleteSource = false)
        {
            // Use multi part as long as the part size is less than the object size
            // and the object is at least 5 MiB, which is the minimum object size for multipart
            return (await CopyOrMoveObjectAsync(client, request, partSize, deleteSource, preferMultipartLogic)).Response;
        }

        /// <summary>
        /// Copies or moves an S3 object to another location. This method prefers using a multipart copy as long as the default part size, 5 MiB, is less
        /// than the source object's size. If the file exists at the destination, if will be overwritten with no warning.
        /// </summary>
        /// <param name="client">The Amazon S3 Client</param>
        /// <param name="request">The Copy Object Request</param>
        /// <param name="deleteSource">If set to true, the source object will be deleted if it was successfully copied. The default is false.</param>
        /// <returns>The copy object response</returns>
        public static async Task<CopyObjectResponse> CopyOrMoveObjectMultipartAsync(this IAmazonS3 client, CopyObjectRequest request, bool deleteSource = false)
        {
            // Use multi part as long as the part size is less than the object size
            // and the object is at least 5 MiB, which is the minimum object size for multipart
            return (await CopyOrMoveObjectAsync(client, request, Constants.FIVE_MEBIBYTE, deleteSource, preferMultipartLogic)).Response;
        }

        /// <summary>
        /// Copies or moves an S3 object to another location. If the object is over 5 GiB, the method automatically handles
        /// using a multipart copy using the part size specified. If the object is under 5 GiB, a single copy operation is performed.
        /// If the file exists at the destination, if will be overwritten with no warning.
        /// </summary>
        /// <param name="client">The Amazon S3 Client</param>
        /// <param name="request">The Copy Object Request</param>
        /// <param name="partSize">The size of the part to use for a multipart copy</param>
        /// <param name="deleteSource">If set to true, the source object will be deleted if it was successfully copied. The default is false.</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <returns>The copy object response</returns>
        public static async Task<CopyObjectResponse> CopyOrMoveObjectAsync(this IAmazonS3 client, CopyObjectRequest request, long partSize, bool deleteSource = false, bool preferMultipart = false)
        {
            if (preferMultipart)
            {
                // Use multi part as long as the part size is less than the object size
                return await CopyOrMoveObjectMultipartAsync(client, request, partSize, deleteSource);
            }
            else
            {
                // Only use multi part if required due to object size
                return (await CopyOrMoveObjectAsync(client, request, partSize, deleteSource, standardMultipartLogic)).Response;
            }
        }

        /// <summary>
        /// Copies or moves an S3 object to another location. If the object is over 5 GiB, the method automatically handles
        /// using a multipart copy using the default part size of 5 MiB. If the file exists at the destination, if will be 
        /// overwritten with no warning.
        /// </summary>
        /// <param name="client">The Amazon S3 Client</param>
        /// <param name="request">The Copy Object Request</param>
        /// <param name="deleteSource">If set to true, the source object will be deleted if it was successfully copied</param>
        /// <param name="preferMultipart">If set to true, the method will use a multipart copy as long as the part size is less than the object size for any object, even
        /// those under 5 GiB.</param>
        /// <returns>The copy object response</returns>
        public static async Task<CopyObjectResponse> CopyOrMoveObjectAsync(this IAmazonS3 client, CopyObjectRequest request, bool deleteSource = false, bool preferMultipart = false)
        {
            if (preferMultipart)
            {
                // Use multi part as long as the part size is less than the object size
                return await CopyOrMoveObjectMultipartAsync(client, request, Constants.FIVE_MEBIBYTE, deleteSource);
            }
            else
            {
                // Use multi part when the object is over 5 GiB
                return await CopyOrMoveObjectAsync(client, request, Constants.FIVE_MEBIBYTE, deleteSource);
            }
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Determines based on the object size and chunk size whether to use a multi-part upload
        /// </summary>
        /// <param name="objectSizeInBytes"></param>
        /// <param name="chunkSizeInBytes"></param>
        /// <returns></returns>
        private static bool UseMultipart(long objectSizeInBytes, long chunkSizeInBytes)
        {
            return objectSizeInBytes >= Constants.MINIMUM_MULTIPART_SIZE && chunkSizeInBytes < objectSizeInBytes;
        }

        /// <summary>
        /// Chunks an IEnumerable into multiple lists of a specified size
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <param name="chunkSize"></param>
        /// <returns></returns>
        private static IEnumerable<List<T>> ChunkList<T>(this IEnumerable<T> input, int chunkSize)
        {
            if (chunkSize <= 0)
            {
                throw new ArgumentOutOfRangeException("chunkSize", "The chunk size must be greater than 0.");
            }

            if (input == null)
            {
                throw new ArgumentNullException("input");
            }

            if (input.Any())
            {
                IEnumerator<T> Enumerator = input.GetEnumerator();
                List<T> ReturnList = new List<T>(chunkSize);
                int Counter = 1;

                while (Enumerator.MoveNext())
                {
                    if (Counter >= chunkSize)
                    {
                        yield return ReturnList;
                        ReturnList = new List<T>();
                        Counter = 1;
                    }

                    ReturnList.Add(Enumerator.Current);
                    Counter++;
                }

                yield return ReturnList;
            }
        }

        /// <summary>
        /// Copies the properties from the provided source object into a new object of the specified destination
        /// type. The properties that are copied are matched by property name, so if both objects have a "Name" property for example,
        /// then the value in the source is set on the destination "Name" property. Properties that cannot be set or 
        /// assigned to in the destination or cannot be read from the source are omitted.
        /// </summary>
        /// <typeparam name="TDestination">The destination type to construct and assign to.</typeparam>
        /// <param name="source">The source object whose properties will be copied</param>
        /// <returns>A new object with the copied property values from matching property names in the source.</returns>
        private static TDestination CopyProperties<TDestination>(this object source, params string[] propertiesToIgnore) where TDestination : class, new()
        {
            IEnumerable<PropertyInfo> Properties = source.GetType().GetRuntimeProperties();

            TDestination Destination = new TDestination();
            Type DestinationType = Destination.GetType();

            if (propertiesToIgnore == null)
            {
                propertiesToIgnore = new string[0];
            }

            foreach (PropertyInfo Info in Properties.Where(x => !propertiesToIgnore.Contains(x.Name, StringComparer.OrdinalIgnoreCase)))
            {
                try
                {
                    // If the property can't be read, just move on to the
                    // next item in the foreach loop
                    if (!Info.CanRead)
                    {
                        continue;
                    }

                    PropertyInfo DestinationProperty = DestinationType.GetProperty(Info.Name);

                    // If the destination is null (property doesn't exist on the object), 
                    // can't be written, or isn't assignable from the source, move on to the next
                    // property in the foreach loop
                    if (DestinationProperty == null ||
                        !DestinationProperty.CanWrite ||
                        (DestinationProperty.GetSetMethod() != null && (DestinationProperty.GetSetMethod().Attributes & MethodAttributes.Static) != 0) ||
                        !DestinationProperty.PropertyType.IsAssignableFrom(Info.PropertyType))
                    {
                        continue;
                    }

                    DestinationProperty.SetValue(Destination, Info.GetValue(source, null));
                }
                catch (Exception)
                {
                    continue;
                }
            }

            return Destination;
        }

        /// <summary>
        /// Converts a CopyObjectRequest object to the specified type by copying over
        /// property values from the source to a new destination object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="request"></param>
        /// <returns></returns>
        private static T ConvertTo<T>(this CopyObjectRequest request, params string[] propertiesToIgnore) where T : class, new()
        {
            return request.CopyProperties<T>(propertiesToIgnore);
        }

        /// <summary>
        /// Provides the actual implementation to move or copy an S3 object
        /// </summary>
        /// <param name="client"></param>
        /// <param name="request"></param>
        /// <param name="partSize"></param>
        /// <param name="deleteSource"></param>
        /// <returns></returns>
        private static async Task<CopyObjectRequestResponse> CopyOrMoveObjectAsync(this IAmazonS3 client, CopyObjectRequest request, long partSize, bool deleteSource, Func<long, long, bool> useMulitpart)
        {
            /// Handle operation cancelled exceptions
            ExponentialBackoffAndRetryClient backoffClient = new ExponentialBackoffAndRetryClient(4, 100, 1000)
            {
                ExceptionHandlingLogic = (ex) =>
                {
                    if (ex is OperationCanceledException)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            };

            try
            {
                ParameterTests.NonNull(request, "request");
                ParameterTests.OutOfRange(partSize >= Constants.MINIMUM_MULTIPART_PART_SIZE, "partSize", $"The part size must be at least {Constants.MINIMUM_MULTIPART_PART_SIZE} bytes.");
                ParameterTests.OutOfRange(partSize <= Constants.MAXIMUM_MULTIPART_PART_SIZE, "partSize", $"The part size cannot exceed {Constants.MAXIMUM_MULTIPART_PART_SIZE} bytes.");

                if (request.SourceKey == request.DestinationKey &&
                    request.SourceBucket != null &&
                    request.SourceBucket.Equals(request.DestinationBucket, StringComparison.OrdinalIgnoreCase))
                {
                    throw new SourceDestinationSameException("The source and destination of the copy operation cannot be the same.", new CopyObjectRequest[] { request });
                }

                // Get the size of the object.
                GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest
                {
                    BucketName = request.SourceBucket,
                    Key = request.SourceKey
                };

                long objectSize;
                GetObjectMetadataResponse metadataResponse;

                try
                {
                    metadataResponse = await backoffClient.RunAsync(() => client.GetObjectMetadataAsync(metadataRequest));
                    objectSize = metadataResponse.ContentLength; // Length in bytes.
                }
                catch (Exception e)
                {
                    throw e;
                }

                CopyObjectResponse response = null;

                if (UseMultipart(objectSize, partSize))
                {
                    // If it takes more than a 5 GiB part to make 10000 or less parts, than this operation
                    // isn't supported for an object this size
                    if (objectSize / partSize > Constants.MAXIMUM_PARTS)
                    {
                        throw new NotSupportedException($"The object size, {objectSize}, cannot be broken into fewer than {Constants.MAXIMUM_PARTS} parts using a part size of {partSize} bytes.");
                    }

                    List<Task<CopyPartResponse>> copyResponses = new List<Task<CopyPartResponse>>();

                    // This property has a nullable backing private field that when set to
                    // anything non-null causes the x-amz-object-lock-retain-until-date
                    // header to be sent which in turn results in an exception being thrown
                    // that the Bucket is missing ObjectLockConfiguration
                    InitiateMultipartUploadRequest initiateRequest = request.ConvertTo<InitiateMultipartUploadRequest>("ObjectLockRetainUntilDate");
                    initiateRequest.BucketName = request.DestinationBucket;
                    initiateRequest.Key = request.DestinationKey;

                    InitiateMultipartUploadResponse initiateResponse = await backoffClient.RunAsync(() => client.InitiateMultipartUploadAsync(initiateRequest));

                    try
                    {
                        long bytePosition = 0;
                        int counter = 1;

                        // Launch all of the copy parts
                        while (bytePosition < objectSize)
                        {
                            CopyPartRequest copyRequest = request.ConvertTo<CopyPartRequest>("ObjectLockRetainUntilDate");
                            copyRequest.UploadId = initiateResponse.UploadId;
                            copyRequest.FirstByte = bytePosition;
                            // If we're on the last part, the last byte is the object size minus 1, otherwise the last byte is the part size minus one
                            // added to the current byte position
                            copyRequest.LastByte = ((bytePosition + partSize - 1) >= objectSize) ? objectSize - 1 : bytePosition + partSize - 1;
                            copyRequest.PartNumber = counter++;

                            copyResponses.Add(backoffClient.RunAsync(() => client.CopyPartAsync(copyRequest)));

                            bytePosition += partSize;
                        }

                        IEnumerable<CopyPartResponse> responses = (await Task.WhenAll(copyResponses)).OrderBy(x => x.PartNumber);

                        // Set up to complete the copy.
                        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest
                        {
                            BucketName = request.DestinationBucket,
                            Key = request.DestinationKey,
                            UploadId = initiateResponse.UploadId
                        };

                        completeRequest.AddPartETags(responses);

                        // Complete the copy.
                        CompleteMultipartUploadResponse completeUploadResponse = await backoffClient.RunAsync(() => client.CompleteMultipartUploadAsync(completeRequest));

                        response = completeUploadResponse.CopyProperties<CopyObjectResponse>();
                        response.SourceVersionId = metadataResponse.VersionId;
                    }
                    catch (AmazonS3Exception e)
                    {
                        AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest()
                        {
                            BucketName = request.DestinationBucket,
                            Key = request.DestinationKey,
                            UploadId = initiateResponse.UploadId
                        };

                        await backoffClient.RunAsync(() => client.AbortMultipartUploadAsync(abortRequest));

                        throw e;
                    }
                }
                else
                {
                    response = await backoffClient.RunAsync(() => client.CopyObjectAsync(request));
                }

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    throw new AmazonS3Exception($"Could not copy object from s3://{request.SourceBucket}/{request.SourceKey} to s3://{request.DestinationBucket}/{request.DestinationKey}. Received response : {(int)response.HttpStatusCode}");
                }
                else
                {
                    // We already checked to make sure the source and destination weren't the same
                    // and it's safe to delete the source object
                    if (deleteSource)
                    {
                        DeleteObjectRequest deleteRequest = new DeleteObjectRequest()
                        {
                            BucketName = request.SourceBucket,
                            Key = request.SourceKey
                        };

                        DeleteObjectResponse deleteResponse = await backoffClient.RunAsync(() => client.DeleteObjectAsync(deleteRequest));

                        if (deleteResponse.HttpStatusCode != HttpStatusCode.NoContent)
                        {
                            throw new AmazonS3Exception($"Could not delete s3://{request.SourceBucket}/{request.SourceKey}. Received response : {(int)deleteResponse.HttpStatusCode}");
                        }
                    }

                    return new CopyObjectRequestResponse(request, response);
                }
            }
            catch (Exception e)
            {
                return null;
            }
        }

        /// <summary>
        /// A simple wrapper to identify whether deletes will be batched or not, all
        /// public methods should call this
        /// </summary>
        /// <param name="client"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        private static async Task<BatchCopyResponse> MoveBatchAsnyc(this IAmazonS3 client, BatchMoveRequest request)
        {
            if (request.BatchDelete)
            {
                return await MoveWithBatchDeleteAsync(client, request);
            }
            else
            {
                return await CoreBatchCopyAsync(client, request, true);
            }
        }

        /// <summary>
        /// Performs a number of async copy object operations in parallel and then a smaller
        /// number of delete operations after the copies complete
        /// </summary>
        /// <param name="client"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        private static async Task<BatchCopyResponse> MoveWithBatchDeleteAsync(this IAmazonS3 client, BatchMoveRequest request)
        {
            List<FailedCopyRequest> failures = new List<FailedCopyRequest>();

            // This method checks for same source/destination problems and will throw an exception
            BatchCopyResponse responses = await client.CoreBatchCopyAsync(request, false);

            // Max keys in a request is 1000
            // Make sure that we don't delete objects that
            // were moved to that same location, this shouldn't
            // happen because of the same logic in the copy operation
            // but make sure
            foreach (IEnumerable<KeyValuePair<CopyObjectRequest, CopyObjectResponse>> responseSet in
                responses.SuccessfulResponses.Where(x => !(x.Key.SourceKey == x.Key.DestinationKey &&
                x.Key.SourceBucket != null &&
                x.Key.SourceBucket.Equals(x.Key.DestinationBucket, StringComparison.OrdinalIgnoreCase))).ChunkList(1000))
            {
                DeleteObjectsRequest delete = new DeleteObjectsRequest()
                {
                    BucketName = request.Requests.First().SourceBucket,
                    Objects = responseSet.Select(x => new KeyVersion() { Key = x.Key.SourceKey, VersionId = x.Key.SourceVersionId }).ToList()
                };

                try
                {
                    DeleteObjectsResponse response = await client.DeleteObjectsAsync(delete);

                    // Find the delete errors and create a new failed copy request
                    // object for each one
                    List<FailedCopyRequest> deleteFailures = response.DeleteErrors
                        .Select(x => new FailedCopyRequest(
                            responseSet.First(y => y.Key.SourceKey == x.Key).Key,
                            new AmazonS3Exception(x.Message) { ErrorCode = x.Code },
                            FailureMode.DELETE)
                        ).ToList();

                    // Remove any items that were successful in the copy
                    // but failed to delete from the successful responses
                    // list and indicate they failed during delete
                    foreach (FailedCopyRequest failure in deleteFailures)
                    {
                        responses.SuccessfulResponses.Remove(failure.Request);
                    }

                    foreach (FailedCopyRequest fail in deleteFailures)
                    {
                        responses.FailedRequests.Add(fail);
                    }
                }
                catch (Exception e)
                {
                    // Remove all the copy responses from the success
                    // group and make them failures when an exception occurs
                    foreach (KeyValuePair<CopyObjectRequest, CopyObjectResponse> item in responseSet)
                    {
                        responses.SuccessfulResponses.Remove(item.Key);
                        responses.FailedRequests.Add(new FailedCopyRequest(item.Key, e, FailureMode.DELETE));
                    }
                }
            }

            return responses;
        }

        /// <summary>
        /// Performs a number of async copy object operations in parallel, all public methods should
        /// call this.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        private static async Task<BatchCopyResponse> CoreBatchCopyAsync(this IAmazonS3 client, BatchCopyRequest request, bool deleteSource)
        {
            ParameterTests.NonNull(request, "request");
            ParameterTests.OutOfRange(request.PartSize >= Constants.MINIMUM_MULTIPART_PART_SIZE, "partSize", $"The part size must be at least {Constants.MINIMUM_MULTIPART_PART_SIZE} bytes.");
            ParameterTests.OutOfRange(request.PartSize <= Constants.MAXIMUM_MULTIPART_PART_SIZE, "partSize", $"The part size cannot exceed {Constants.MAXIMUM_MULTIPART_PART_SIZE} bytes.");

            // Make sure there are not requests that have the same source and destination
            IEnumerable<CopyObjectRequest> errors = request.Requests
                .Where(x => x.SourceKey == x.DestinationKey && x.SourceBucket != null && x.SourceBucket.Equals(x.DestinationBucket, StringComparison.OrdinalIgnoreCase));

            if (errors.Any())
            {
                throw new SourceDestinationSameException($"The batch copy/move operation contained requests that had the same source and destination and could cause the accidential loss of data.", errors);
            }

            List<CopyObjectRequestResponse> responses = new List<CopyObjectRequestResponse>();
            List<FailedCopyRequest> failures = new List<FailedCopyRequest>();

            // Don't copy objects that have the same source and destination
            // object keys are case sensitive, but bucket names are not, they
            // are all supposed to be lower case
            IEnumerable<CopyObjectRequest> filtered = request.Requests.Where(x => !(x.SourceKey == x.DestinationKey && x.SourceBucket != null && x.SourceBucket.Equals(x.DestinationBucket, StringComparison.OrdinalIgnoreCase)));

            int counter = 0;

            //IEnumerable<>
            foreach (List<CopyObjectRequest> chunk in filtered.ChunkList(100))
            {
                Debug.WriteLine($"Processing request chunk {++counter}.");

                List<Task<CopyObjectRequestResponse>> insideLoop = new List<Task<CopyObjectRequestResponse>>();

                foreach (CopyObjectRequest req in chunk)
                {
                    try
                    {
                        if (request.PreferMultipart)
                        {
                            insideLoop.Add(client.CopyOrMoveObjectAsync(req, request.PartSize, deleteSource, preferMultipartLogic));
                        }
                        else
                        {                          
                            insideLoop.Add(client.CopyOrMoveObjectAsync(req, request.PartSize, deleteSource, standardMultipartLogic));
                        }
                    }
                    catch (Exception e)
                    {
                        failures.Add(new FailedCopyRequest(req, e, FailureMode.COPY));
                    }
                }

                try
                {
                    IEnumerable<CopyObjectRequestResponse> responseChunk = await Task.WhenAll(insideLoop);
                    responses.AddRange(responseChunk);
                }
                catch (Exception e)
                {
                    failures.Add(new FailedCopyRequest(null, e, FailureMode.COPY));
                }
            }

            try
            {
                var dict = responses.ToDictionary(x => x.Request, x => x.Response);

                return new BatchCopyResponse(dict, failures);
            }
            catch (Exception e)
            {
                return null;
            }
        }

        #endregion
    }
}
