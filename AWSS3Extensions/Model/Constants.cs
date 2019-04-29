namespace BAMCIS.AWSS3Extensions.Model
{
    /// <summary>
    /// Provides constant values
    /// </summary>
    internal static class Constants
    {
        #region Internal Fields

        internal const long FIVE_MEBIBYTE = 5242880; // 5 MiB
        internal const long FIVE_GIBIBYTE = 5368709120; // 5 GiB
        internal const long MINIMUM_MULTIPART_SIZE = FIVE_MEBIBYTE; // 5 MiB
        internal const long MINIMUM_MULTIPART_PART_SIZE = FIVE_MEBIBYTE;
        internal const long MAXIMUM_MULTIPART_PART_SIZE = FIVE_GIBIBYTE;
        internal const int MAXIMUM_PARTS = 10000;

        #endregion
    }
}
