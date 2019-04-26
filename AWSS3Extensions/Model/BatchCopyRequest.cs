using Amazon.S3.Model;
using System;
using System.Collections.Generic;

namespace BAMCIS.AWSS3FastCopy.Model
{
    public class BatchCopyRequest
    {
        #region Public Properties

        /// <summary>
        /// The copy object requests to execute
        /// </summary>
        public IEnumerable<CopyObjectRequest> Requests { get; set; }

        /// <summary>
        /// The size of the part to use for a multipart copy
        /// </summary>
        public long PartSize { get; set; }

        /// <summary>
        /// If set to true, the method will use a multipart copy as long as the part 
        /// size is less than the object size for any object, even those under 5 GiB
        /// </summary>
        public bool PreferMultipart { get; set; }
       
        /// <summary>
        /// The maximum number of concurrent copy operations
        /// </summary>
        public int MaxConcurrency { get; set; }

        #endregion

        #region Private Fields

        /// <summary>
        /// A function that determines whether to use multi-part upload for an objet
        /// </summary>
        private Func<long, long, bool> UseMultipart { get; set; }

        #endregion

        #region Construtors

        /// <summary>
        /// Default constructor, sets a default of 5 MiB part size,
        /// a max concurrency of 100, and sets PreferMultipart to false.
        /// </summary>
        /// <param name="requests"></param>
        /// <param name="useMultipart"></param>
        public BatchCopyRequest(IEnumerable<CopyObjectRequest> requests)
        {
            this.Requests = requests ?? throw new ArgumentNullException("requests");
            this.PartSize = Constants.FIVE_MEBIBYTE;
            this.MaxConcurrency = -1;
            this.PreferMultipart = false;
        }

        #endregion
    }
}
