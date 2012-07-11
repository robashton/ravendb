namespace RavenFS.Synchronization
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Linq;
	using Extensions;
	using NLog;
	using RavenFS.Client;

	public class SynchronizationQueue
	{
		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly ConcurrentDictionary<string, ConcurrentQueue<SynchronizationWorkItem>> pendingSynchronizations =
			new ConcurrentDictionary<string, ConcurrentQueue<SynchronizationWorkItem>>();

		private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationWorkItem>> activeSynchronizations =
			new ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationWorkItem>>();

		public IEnumerable<SynchronizationDetails> Pending
		{
			get
			{
				return from destinationPending in pendingSynchronizations
				       from pendingFile in destinationPending.Value
				       select new SynchronizationDetails
				              	{
				              		DestinationUrl = destinationPending.Key,
				              		FileName = pendingFile.FileName,
									Type = pendingFile.SynchronizationType,
									FileETag = pendingFile.FileETag
				              	};
			}
		}

		public IEnumerable<SynchronizationDetails> Active
		{
			get
			{
				return from destinationActive in activeSynchronizations
				       from activeFile in destinationActive.Value
				       select new SynchronizationDetails
				              	{
				              		DestinationUrl = destinationActive.Key,
				              		FileName = activeFile.Key,
				              		Type = activeFile.Value.SynchronizationType,
				              		FileETag = activeFile.Value.FileETag
				              	};
			}
		}

		public int NumberOfActiveSynchronizationTasksFor(string destination)
		{
			return activeSynchronizations.GetOrAdd(destination, new ConcurrentDictionary<string, SynchronizationWorkItem>()).Count;
		}

		public void EnqueueSynchronization(string destination, SynchronizationWorkItem workItem)
		{
			var pendingForDestination = pendingSynchronizations.GetOrAdd(destination, new ConcurrentQueue<SynchronizationWorkItem>());

			foreach (var pendingWork in pendingForDestination)
			{
				// if there is a file in pending synchronizations do not add it again
				if (pendingWork.Equals(workItem))
				{
					log.Debug("{0} for a file {1} and a destination {2} was already existed in a pending queue", workItem.GetType().Name, workItem.FileName, destination);
					return;
				}

				// if there is a work of the same type but for lower file ETag just refresh work metadata
				if (pendingWork.SynchronizationType == workItem.SynchronizationType &&
					Buffers.Compare(workItem.FileETag.ToByteArray(), pendingWork.FileETag.ToByteArray()) > 0)
				{
					pendingWork.RefreshMetadata();
					log.Debug("{0} for a file {1} and a destination {2} was already existed in a pending queue byt with older ETag, it's metadata has been refreshed", workItem.GetType().Name, workItem.FileName, destination);
					return;
				}
			}

			var activeForDestination = activeSynchronizations.GetOrAdd(destination, new ConcurrentDictionary<string, SynchronizationWorkItem>());

			// if there is a file in pending synchronizations do not add it again
			if (activeForDestination.ContainsKey(workItem.FileName) && activeForDestination[workItem.FileName].Equals(workItem))
			{
				log.Debug("{0} for a file {1} and a destination {2} was already existed in an active queue", workItem.GetType().Name, workItem.FileName, destination);
				return;
			}

			pendingForDestination.Enqueue(workItem);
			log.Debug("{0} for a file {1} and a destination {2} was enqueued", workItem.GetType().Name, workItem.FileName, destination);
		}

		public bool TryDequeuePendingSynchronization(string destination, out SynchronizationWorkItem workItem)
		{
			ConcurrentQueue<SynchronizationWorkItem> pendingForDestination;
			if (pendingSynchronizations.TryGetValue(destination, out pendingForDestination) == false)
			{
				log.Warn("Could not get a pending synchronization queue for {0}", destination);
				workItem = null;
				return false;
			}

			return pendingForDestination.TryDequeue(out workItem);
		}

		public bool IsDifferentWorkForTheSameFileBeingPerformed(SynchronizationWorkItem work, string destination)
		{
			ConcurrentDictionary<string, SynchronizationWorkItem> activeForDestination;
			if(!activeSynchronizations.TryGetValue(destination, out activeForDestination))
			{
				return false;
			}

			SynchronizationWorkItem activeWork;
			return activeForDestination.TryGetValue(work.FileName, out activeWork) && !activeWork.Equals(work);
		}

		public void SynchronizationStarted(SynchronizationWorkItem work, string destination)
		{
			var activeForDestination = activeSynchronizations.GetOrAdd(destination, new ConcurrentDictionary<string, SynchronizationWorkItem>());

			if(activeForDestination.TryAdd(work.FileName, work))
			{
				log.Debug("File '{0}' with ETag {1} was added to an active synchronization queue for a destination {2}", work.FileName,
				          work.FileETag, destination);
			}
		}

		public void SynchronizationFinished(SynchronizationWorkItem work, string destination)
		{
			ConcurrentDictionary<string, SynchronizationWorkItem> activeDestinationTasks;

			if (activeSynchronizations.TryGetValue(destination, out activeDestinationTasks) == false)
			{
				log.Warn("Could not get an active synchronization queue for {0}", destination);
				return;
			}
			
			SynchronizationWorkItem removingItem;
			if(activeDestinationTasks.TryRemove(work.FileName, out removingItem))
			{
				log.Debug("File '{0}' with ETag {1} was removed from an active synchronization queue for a destination {2}", work.FileName,
						  work.FileETag, destination);
			}
		}
	}
}