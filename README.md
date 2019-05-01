# BAMCIS AWS S3 Extension Methods

Extension methods for the `IAmazonS3` interface. This library adds methods for some common tasks like listing all objects in a bucket, emptying a bucket, testing for a bucket's existence, and moving (or renaming) or copying items in bulk from one location to another. For move and copy operations, the extension methods simplify using multi-part operations and chunking the data. Specifically for move operations, the extension methods handle deleting the source item after the copy successfully completes.

## Table of Contents
- [Usage](#usage)
- [Revision History](#revision-history)

## Usage

Import the package:

    using BAMCIS.AWSS3Extensions;

A simple copy example:

    CopyObjectRequest request = new CopyObjectRequest()
    {
        DestinationBucket = "dest-bucket",
        SourceBucket = "src-bucket",
        SourceKey = "testfile.bin",
        DestinationKey = "testfile-copy.bin",
    };

    CopyObjectResponse Response = await client.CopyOrMoveObjectAsync(request, 16777216, false);

The copy operation uses a 16 MiB part size for the copy operation, so if `testfile.bin` is larger than 16 MiB, it will use multi-part copy, you don't have to worry about splitting the file and initiating or finishing the multi-part copy request. The `false` parameter specifies that the file shouldn't be deleted after the copy operation completes successfully.


This example renames all of the objects in a bucket:

    IEnumerable<S3Object> objects = (await client.ListAllObjectsAsync(list)).SelectMany(x => x.S3Objects);

    BulkMoveRequest request = new BulkMoveRequest(objects.Select(x => new CopyObjectRequest()
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

    BulkCopyResponse response = await client.BulkMoveAsync(request);

The example first enumerates all objects in a bucket using one of the included extension methods `ListAllObjectsAsync`. Then it creates a `BulkMoveRequest` by using the list of `S3Object` items to create a CopyObjectRequest for each one. The `BulkMoveAsync` operation renames all of the objects in the bucket by prepending "moved_" and also assigns them to the One Zone IA storage class. 

## Revision History

### 1.0.0
Initial release of the library.
