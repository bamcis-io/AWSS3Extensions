using Amazon.S3.Model;
using System.Collections.Generic;

namespace BAMCIS.AWSS3Extensions.Model
{
    /// <summary>
    /// A request to bulk move (or rename) 1 or more S3 objects.
    /// </summary>
    public class BulkMoveRequest : BulkCopyRequest
    {
        #region Public Properties

        /// <summary>
        /// If set to true, the default, requests in batches up to 1000 will be sent for deletes. 
        /// If set to false, a delete request is made per object that is being moved.
        /// </summary>
        public bool BulkDelete { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor, sets a default of 5 MiB part size and
        /// a max concurrency of 100.
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="useMultipart"></param>
        public BulkMoveRequest(IEnumerable<CopyObjectRequest> requests) : base(requests)
        {
            this.BulkDelete = true;
        }

        #endregion
    }
}
