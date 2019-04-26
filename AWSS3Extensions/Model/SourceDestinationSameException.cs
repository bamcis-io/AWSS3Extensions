using Amazon.S3;
using Amazon.S3.Model;
using System.Collections.Generic;

namespace BAMCIS.AWSS3FastCopy.Model
{
    /// <summary>
    /// Represents an exception with the source bucket and key are the same
    /// as the destination, which could lead to data being accidentially
    /// deleted during a move operation
    /// </summary>
    public class SourceDestinationSameException : AmazonS3Exception
    {
        #region Public Properties

        /// <summary>
        /// The requests that could not be completed because their source 
        /// and destination were the same
        /// </summary>
        public IEnumerable<CopyObjectRequest> InvalidRequests { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="message"></param>
        /// <param name="invalidRequests"></param>
        public SourceDestinationSameException(string message, IEnumerable<CopyObjectRequest> invalidRequests) : base(message)
        {
            this.InvalidRequests = invalidRequests;
        }

        #endregion
    }
}
