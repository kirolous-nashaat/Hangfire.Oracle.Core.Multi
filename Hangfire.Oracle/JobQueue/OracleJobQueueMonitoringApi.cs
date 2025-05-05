using System;
using System.Collections.Generic;
using System.Linq;

using Dapper;

namespace Hangfire.Oracle.Core.JobQueue
{
    internal class OracleJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();
        private List<string> _queuesCache = new List<string>();
        private DateTime _cacheUpdated;

        private readonly OracleStorage _storage;
        private readonly string prefix;
        public OracleJobQueueMonitoringApi(OracleStorage storage, string prefix)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            this.prefix = prefix;
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    var result = _storage.UseConnection(connection =>
                    {
                        return connection.Query($"SELECT DISTINCT(QUEUE) as QUEUE FROM {prefix}HF_JOB_QUEUE").Select(x => (string)x.QUEUE).ToList();
                    });

                    _queuesCache = result;
                    _cacheUpdated = DateTime.UtcNow;
                }

                return _queuesCache.ToList();
            }
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            string sqlQuery = $@"
SELECT JOB_ID AS JobId
  FROM (SELECT JOB_ID, RANK () OVER (ORDER BY ID) AS RANK
          FROM {prefix}HF_JOB_QUEUE
         WHERE QUEUE = :QUEUE)
 WHERE RANK BETWEEN :S AND :E
";

            return _storage.UseConnection(connection =>
                connection.Query<int>(sqlQuery, new { QUEUE = queue, S = from + 1, E = from + perPage }));
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return Enumerable.Empty<int>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return _storage.UseConnection(connection =>
            {
                var result = connection.QuerySingle<int>($"SELECT COUNT(ID) FROM {prefix}HF_JOB_QUEUE WHERE QUEUE = :QUEUE", new { QUEUE = queue });

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result
                };
            });
        }
    }
}