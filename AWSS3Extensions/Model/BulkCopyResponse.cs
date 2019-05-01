using Amazon.S3.Model;
using System.Collections.Generic;
using System.Linq;

namespace BAMCIS.AWSS3Extensions.Model
{
    /// <summary>
    /// A response from a BulkCopyRequest
    /// </summary>
    public class BulkCopyResponse
    {
        #region Public Properties

        /// <summary>
        /// Successfully copied responses
        /// </summary>
        public Dictionary<CopyObjectRequest, CopyObjectResponse> SuccessfulOperations { get; set; }

        /// <summary>
        /// Failed copy operations
        /// </summary>
        public List<FailedCopyRequest> FailedRequests { get; }

        #endregion

        #region Construtors

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="requestResponsePairs"></param>
        /// <param name="failures"></param>
        public BulkCopyResponse(Dictionary<CopyObjectRequest, CopyObjectResponse> requestResponsePairs, IEnumerable<FailedCopyRequest> failures)
        {
            this.SuccessfulOperations = requestResponsePairs ?? new Dictionary<CopyObjectRequest, CopyObjectResponse>();
            this.FailedRequests = failures?.ToList() ?? new List<FailedCopyRequest>();
        }

        #endregion
    }
}
