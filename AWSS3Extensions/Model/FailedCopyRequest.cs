using Amazon.S3.Model;
using System;

namespace BAMCIS.AWSS3Extensions.Model
{
    /// <summary>
    /// Represents a failed copy or move request
    /// </summary>
    public class FailedCopyRequest
    {
        #region Public Properties

        /// <summary>
        /// The request that caused a failure.
        /// </summary>
        public CopyObjectRequest Request { get; }

        /// <summary>
        /// The exception raised by the request.
        /// </summary>
        public Exception FailureReason { get; }

        /// <summary>
        /// The operation that caused the failure.
        /// </summary>
        public FailureMode FailureMode { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="request">The request that was executed and failed</param>
        /// <param name="exception">The exception that was raised</param>
        internal FailedCopyRequest(CopyObjectRequest request, Exception exception, FailureMode mode)
        {
            this.Request = request ?? throw new ArgumentNullException("request");
            this.FailureReason = exception ?? throw new ArgumentNullException("exception");
            this.FailureMode = mode;
        }

        #endregion
    }
}
