﻿using System.Threading;
using RavenFS.Extensions;
using RavenFS.Storage;

namespace RavenFS.Synchronization
{
	public class SynchronizationHiLo
	{
		private const long Capacity = 1024*16;
		private readonly object _generatorLock = new object();

		private readonly TransactionalStorage _storage;
		private long _currentHi;
		private long _currentLo = Capacity + 1;

		public SynchronizationHiLo(TransactionalStorage storage)
		{
			_storage = storage;
		}

		public long NextId()
		{
			var incrementedCurrentLow = Interlocked.Increment(ref _currentLo);
			if (incrementedCurrentLow > Capacity)
			{
				lock (_generatorLock)
				{
					if (Thread.VolatileRead(ref _currentLo) > Capacity)
					{
						_currentHi = GetNextHi();
						_currentLo = 1;
						incrementedCurrentLow = 1;
					}
				}
			}
			return (_currentHi - 1)*Capacity + (incrementedCurrentLow);
		}

		private long GetNextHi()
		{
			long result = 0;
			_storage.Batch(
				accessor =>
					{
						accessor.TryGetConfigurationValue(SynchronizationConstants.RavenSynchronizationVersionHiLo, out result);
						result++;
						accessor.SetConfigurationValue(SynchronizationConstants.RavenSynchronizationVersionHiLo, result);
					});
			return result;
		}
	}
}