using Amazon.S3.Model;
using System;

namespace BAMCIS.AWSS3FastCopy.Model
{
    /// <summary>
    /// A wrapper to contain both the copy request and response
    /// </summary>
    internal class CopyObjectRequestResponse
    {
        #region Public Properties

        /// <summary>
        /// The CopyObjectResponse from the copy operation
        /// </summary>
        internal CopyObjectResponse Response { get; set; }

        /// <summary>
        /// The CopyObjectRequest that initiated the copy operation
        /// </summary>
        internal CopyObjectRequest Request { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new request/response object
        /// </summary>
        /// <param name="request"></param>
        /// <param name="response"></param>
        internal CopyObjectRequestResponse(CopyObjectRequest request, CopyObjectResponse response)
        {
            this.Request = request ?? throw new ArgumentNullException("request");
            this.Response = response ?? throw new ArgumentNullException("response");
        }

        #endregion
    }
}
