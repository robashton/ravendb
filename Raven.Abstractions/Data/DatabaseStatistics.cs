//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class DatabaseStatistics
	{
		public Guid LastDocEtag { get; set; }
		public Guid LastAttachmentEtag { get; set; }
		public int CountOfIndexes { get; set; }
		public int InMemoryIndexingQueueSize { get; set; }
		public long ApproximateTaskCount { get; set; }

		public long CountOfDocuments { get; set; }

		public string[] StaleIndexes { get; set; }

		public int CurrentNumberOfItemsToIndexInSingleBatch { get; set; }
		
		public int CurrentNumberOfItemsToReduceInSingleBatch { get; set; }

		public decimal DatabaseTransactionVersionSizeInMB { get; set; }

		public IndexStats[] Indexes { get; set; }

		public ServerError[] Errors { get; set; }

		public TriggerInfo[] Triggers { get; set; }

		public IEnumerable<ExtensionsLog> Extensions { get; set; }

		public class TriggerInfo
		{
			public string Type { get; set; }
			public string Name { get; set; }
		}

		public ActualIndexingBatchSize[] ActualIndexingBatchSize { get; set; }
		public FutureBatchStats[] Prefetches { get; set; }
	}

	public class ActualIndexingBatchSize
	{
		public int Size { get; set; }
		public DateTime Timestamp { get; set; }
	}

	public class FutureBatchStats
	{
		public DateTime Timestamp { get; set; }
		public TimeSpan? Duration { get; set; }
		public int? Size { get; set; }
		public int Retries { get; set; }
	}

	public class ExtensionsLog
	{
		public string Name { get; set; }
		public ExtensionsLogDetail[] Installed { get; set; }
	}

	public class ExtensionsLogDetail
	{
		public string Name { get; set; }
		public string Assembly { get; set; }
	}
}
