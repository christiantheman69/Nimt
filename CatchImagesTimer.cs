using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Azure.Storage.Queues;
using Azure.Data.Tables;

namespace Avensia.Nimt
{
    public class CatchImagesTimer
    {
        private readonly ILogger _logger;

        private DateTime lastRun = DateTime.Today;

        public CatchImagesTimer(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<CatchImagesTimer>();
        }

        [Function("CatchImagesTimer")]
        public void Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Get the last run date and time from the storage table
            lastRun = GetLastRunDateTime();
                        
            // Attach to the file share and download the images
            List<string> files = GetPhotosToConvert();

            // Queue the images for processing
            QueueImagesForProcessing(files);

            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }
        }

        private DateTime GetLastRunDateTime()
        {
            string tableConnectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");
            string tableName = "CatchImagesTimer";
            var tableClient = new TableClient(tableConnectionString, tableName);
            tableClient.CreateIfNotExists();
            var record = tableClient.GetEntity<TableEntity>("CatchImagesTimer", "NextRun");
            var r = DateTime.Parse(record.Value["NextRun"]?.ToString() ?? DateTime.Today.ToString());
            return r;
        }

        private void QueueImagesForProcessing(List<string> files)
        {
            string connectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");
            string queueName = "image-processing-queue";

            QueueClient queueClient = new QueueClient(connectionString, queueName);

            foreach (string file in files)
            {
                queueClient.SendMessage(file);
            }
        }

        private List<string> GetPhotosToConvert()
        {
            string connectionString = Environment.GetEnvironmentVariable("TABLE_CONNECTION_STRING");
            string shareName = "nimt-photos";

            // A list of files that has been updated since the last execution date and time.
            List<string> files = new List<string>();

            // The next execution date and time based on the latest file found.
            DateTime nextRun = DateTime.MinValue;

            // Connect to the file share in Azure Storage and start browsing the orginials folder.
            ShareClient share = new ShareClient(connectionString, shareName);
            var remaining = new Queue<ShareDirectoryClient>();
            remaining.Enqueue(share.GetDirectoryClient("originals"));

            while (remaining.Count > 0)
            {
                // Get all of the next directory's files and subdirectories
                ShareDirectoryClient dir = remaining.Dequeue();
                foreach (ShareFileItem item in dir.GetFilesAndDirectories())
                {
                    // Keep walking down directories
                    if (item.IsDirectory)
                    {
                        remaining.Enqueue(dir.GetSubdirectoryClient(item.Name));
                    }
                    else
                    {
                        // It's a file. Check file type
                        if (item.Name.ToLower().EndsWith(".jpg") || item.Name.ToLower().EndsWith(".jpeg"))
                        {
                            var fileClient = dir.GetFileClient(item.Name);
                            string uri = fileClient.Uri.ToString();
                            var fileProperties = fileClient.GetProperties();

                            // If the file has been updated since the last run, add it to the list
                            if (fileProperties.Value.LastModified.DateTime > lastRun)
                            {
                                files.Add(uri);

                                // Save the latest file date and time for the next run.
                                if (fileProperties.Value.LastModified.DateTime > nextRun)
                                    nextRun = fileProperties.Value.LastModified.DateTime;
                            }
                        }
                    }
                }
            }

            // Update the last run date and time to the storage table for the next run (only when there is a new date and time).
            if (nextRun != DateTime.MinValue)
                SetNextRunDateTime(nextRun);

            return files;
        }

        private void SetNextRunDateTime(DateTime nextRun)
        {
            var tableConnectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");
            var tableName = "CatchImagesTimer";
            var tableClient = new TableClient(tableConnectionString, tableName);
            tableClient.CreateIfNotExists();

            var entity = new TableEntity("CatchImagesTimer", "NextRun")
            {
                { "NextRun", nextRun.ToString() }
            };

            tableClient.UpsertEntity(entity);
        }
    }
}
