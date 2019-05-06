# BAMCIS AWS S3 Extension Methods

Extension methods for the `IAmazonS3` interface. This library adds methods for some common tasks like listing all objects in a bucket, emptying a bucket, testing for a bucket's existence, and moving (or renaming) or copying items in bulk from one location to another. For move and copy operations, the extension methods simplify using multi-part operations and chunking the data. Specifically for move operations, the extension methods handle deleting the source item after the copy successfully completes.

## Table of Contents
- [All Methods](#all-methods)
- [Usage](#usage)
  * [Copy or Move Operations](#copy-or-move-operations)
  * [Bulk Operations](#bulk-operations)
  * [Convenience Methods](#convenience-methods)
- [Revision History](#revision-history)

## All Methods
These are all of the methods this library contains to extend the IAmazonS3 client.

* `BucketExistsAsync()`
* `EmptyBucketAsync()`
* `ListAllObjectsAsync()`
* `BulkCopyAsnyc()`
* `BulkMoveAsync()`
* `CopyOrMoveObjectMultipartAsync()`
* `CopyOrMoveObjectAsync()`

There are a number of different signatures for the move and copy operations to give you control over whether it is a copy or move operation, the multi-part chunk size to use, and whether multi-part operations are preferred even when not required.

## Usage

Import the package:

    using BAMCIS.AWSS3Extensions;

### Copy or Move Operations

These methods simplify the code needed to copy or move an S3 object to another location. Each method decides if it needs to use a multipart copy or a single copy operation to move the object and also deletes the source object during a move. You can also specify the part size to use during a multipart copy, the minimum is 5 MiB and the maximum is 5 GiB.

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

In the following examples the `req` variable is a `CopyObjectRequest` object from the AWS S3 SDK. 

This will move the object and only use multipart if the object is over 5 GiB and will use the default 5 MiB part size.

    IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req, true);

To only copy and not move (move deletes the source object, copy does not), specify `false` for the last parameter, or don't specify anything as it defaults to `false`.

    IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req);

This will move the object and use a part size of 16 MiB during a multipart copy if the object is over 5 GiB in size.

    IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req, 16777216, true);

This will move the object and force a multipart copy using the default multipart size, 5 MiB, as long as the part size is less than the object's size.

	IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req, true, true);

This will move the object and force a multipart copy using the specified part size, 16 MiB.

    IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = await client.CopyOrMoveObjectAsync(req, 16777216, true, true);

For the last two examples, there is another method that accomplishes the same task, it removes the need to second boolean `true` value in the method signature. This will still use the single operation copy if the source object doesn't support multipart copy, i.e. it is less than 5 MiB, or your part size is greater than the object's size.

    IAmazonS3 client = new AmazonS3Client();
    CopyObjectResponse response = CopyOrMoveObjectMultipartAsync(req, true);

OR

    CopyObjectResponse response = CopyOrMoveObjectMultipartAsync(req, 16777216, true);

### Bulk Operations

The bulk operations are very similar to the individual copy and move operations.

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

You can specify the same multi-part options for the entire bulk move or copy as you can for the individual operations. The above example uses the default 5 MiB chunk size and prefers to use multi-part, so any object over 5 MiB will be moved using the multi-part copy operation.

 ### Convenience Methods

There are three additional convenience methods

* `BucketExistsAsync()` - Tests if a bucket exists.
* `EmptyBucketAsync()` - Deletes all items in a bucket.
* `ListAllObjectsAsync()` - Lists all objects in a bucket (when a bucket contains more than 1000 objects). Be careful using this method on extremely large buckets.

## Revision History

### 1.0.1
Added "async" to appropriate method names.

### 1.0.0
Initial release of the library.
