//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Implements Unit of Work for accessing a set of sharded RavenDB servers
	/// </summary>
	public class ShardedDocumentSession : BaseShardedDocumentSession<IDatabaseCommands>, IDocumentQueryGenerator,
	                                      IDocumentSessionImpl, ISyncAdvancedSessionOperation
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentSession"/> class.
		/// </summary>
		/// <param name="shardStrategy">The shard strategy.</param>
		/// <param name="shardDbCommands">The shard IDatabaseCommands.</param>
		/// <param name="id"></param>
		/// <param name="documentStore"></param>
		/// <param name="listeners"></param>
		public ShardedDocumentSession(string dbName, ShardedDocumentStore documentStore, DocumentSessionListeners listeners, Guid id,
		                              ShardStrategy shardStrategy, IDictionary<string, IDatabaseCommands> shardDbCommands)
			: base(dbName, documentStore, listeners, id, shardStrategy, shardDbCommands) { }

		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			var shardRequestData = new ShardRequestData
			{
				EntityType = typeof(object),
				Keys = { documentKey }
			};
			var dbCommands = GetCommandsToOperateOn(shardRequestData);

			var documents = shardStrategy.ShardAccessStrategy.Apply(dbCommands,
			                                                        shardRequestData,
			                                                        (commands, i) => commands.Get(documentKey));

			var document = documents.FirstOrDefault(x => x != null);
			if (document != null)
				return document;

			throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
		}

		protected override string GenerateKey(object entity)
		{
			var shardId = shardStrategy.ShardResolutionStrategy.MetadataShardIdFor(entity);
			IDatabaseCommands value;
			if (shardDbCommands.TryGetValue(shardId, out value) == false)
				throw new InvalidOperationException("Could not find shard: " + shardId);
			return Conventions.GenerateDocumentKey(dbName,value, entity);
		}

		protected override Task<string> GenerateKeyAsync(object entity)
		{
			throw new NotSupportedException("Cannot generate key asynchronously using synchronous session");
		}

		#region Properties to access different interfacess

		ISyncAdvancedSessionOperation IDocumentSession.Advanced
		{
			get { return this; }
		}

		/// <summary>
		/// Access the lazy operations
		/// </summary>
		public ILazySessionOperations Lazily
		{
			get { return this; }
		}

		/// <summary>
		/// Access the eager operations
		/// </summary>
		IEagerSessionOperations ISyncAdvancedSessionOperation.Eagerly
		{
			get { return this; }
		}

		#endregion

		#region Load and Include

		#region Synchronous

		public T Load<T>(string id)
		{
			object existingEntity;
			if (entitiesByKey.TryGetValue(id, out existingEntity))
			{
				return (T) existingEntity;
			}

			IncrementRequestCount();
			var shardRequestData = new ShardRequestData
			{
				EntityType = typeof(T),
				Keys = { id }
			};
			var dbCommands = GetCommandsToOperateOn(shardRequestData);
			var results = shardStrategy.ShardAccessStrategy.Apply(dbCommands, shardRequestData, (commands, i) =>
			{
				var loadOperation = new LoadOperation(this, commands.DisableAllCaching, id);
				bool retry;
				do
				{
					loadOperation.LogOperation();
					using (loadOperation.EnterLoadContext())
					{
						retry = loadOperation.SetResult(commands.Get(id));
					}
				} while (retry);
				return loadOperation.Complete<T>();
			});

			var shardsContainThisDocument = results.Where(x => !Equals(x, default(T))).ToArray();
			if (shardsContainThisDocument.Count() > 1)
			{
				throw new InvalidOperationException("Found document with id: " + id +
				                                    " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
			}

			return shardsContainThisDocument.FirstOrDefault();
		}

		public T[] Load<T>(params string[] ids)
		{
			return LoadInternal<T>(ids);
		}

		public T[] Load<T>(IEnumerable<string> ids)
		{
			return LoadInternal<T>(ids.ToArray());
		}

		public T Load<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Load<T>(documentKey);
		}

		public T[] Load<T>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<T>(documentKeys);
		}

		public T[] Load<T>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<T>(documentKeys);
		}

		public T[] LoadInternal<T>(string[] ids)
		{
			return LoadInternal<T>(ids, new string[0]);
		}

		public T[] LoadInternal<T>(string[] ids, string[] includes)
		{
			var results = new T[ids.Length];
			var idsToLoad = GetIdsThatNeedLoading<T>(ids, includes);

			if (!idsToLoad.Any())
				return results;

			IncrementRequestCount();

			foreach (var shard in idsToLoad)
			{
				var currentShardIds = shard.Select(x => x.Id).ToArray();
				var multiLoadOperations = shardStrategy.ShardAccessStrategy.Apply(shard.Key, new ShardRequestData
				{
					EntityType = typeof(T),
					Keys = currentShardIds.ToList()
				}, (dbCmd, i) =>
				{
					var multiLoadOperation = new MultiLoadOperation(this, dbCmd.DisableAllCaching, currentShardIds, includes);
					MultiLoadResult multiLoadResult;
					do
					{
						multiLoadOperation.LogOperation();
						using (multiLoadOperation.EnterMultiLoadContext())
						{
							multiLoadResult = dbCmd.Get(currentShardIds, includes);
						}
					} while (multiLoadOperation.SetResult(multiLoadResult));
					return multiLoadOperation;
				});
				foreach (var multiLoadOperation in multiLoadOperations)
				{
					var loadResults = multiLoadOperation.Complete<T>();
					for (int i = 0; i < loadResults.Length; i++)
					{
						if (ReferenceEquals(loadResults[i], null))
							continue;
						var id = currentShardIds[i];
						var itemPosition = Array.IndexOf(ids, id);
						if (ReferenceEquals(results[itemPosition], default(T)) == false)
						{
							throw new InvalidOperationException("Found document with id: " + id +
							                                    " on more than a single shard, which is not allowed. Document keys have to be unique cluster-wide.");
						}
						results[itemPosition] = loadResults[i];
					}
				}
			}
			return ids.Select(id => // so we get items that were skipped because they are already in the session cache
			{
				object val;
				entitiesByKey.TryGetValue(id, out val);
				return (T) val;
			}).ToArray();
		}

		public ILoaderWithInclude<object> Include(string path)
		{
			return new MultiLoaderWithInclude<object>(this).Include(path);
		}

		public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
		{
			return new MultiLoaderWithInclude<T>(this).Include(path);
		}

		public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
		{
			return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
		}

		#endregion

		#region Lazy loads

		/// <summary>
		/// Loads the specified ids and a function to call when it is evaluated
		/// </summary>
		Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids, Action<T[]> onEval)
		{
			return LazyLoadInternal(ids.ToArray(), new string[0], onEval);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
		{
			return Lazily.Load<T>(ids, null);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		Lazy<T[]> ILazySessionOperations.Load<T>(params string[] ids)
		{
			return Lazily.Load<T>(ids, null);
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		Lazy<T> ILazySessionOperations.Load<T>(string id)
		{
			return Lazily.Load(id, (Action<T>) null);
		}

		/// <summary>
		/// Loads the specified id and a function to call when it is evaluated
		/// </summary>
		Lazy<T> ILazySessionOperations.Load<T>(string id, Action<T> onEval)
		{
			var cmds = GetCommandsToOperateOn(new ShardRequestData
			{
				Keys = { id },
				EntityType = typeof(T)
			});

			var lazyLoadOperation = new LazyLoadOperation<T>(id, new LoadOperation(this, () =>
			{
				var list = cmds.Select(databaseCommands => databaseCommands.DisableAllCaching()).ToList();
				return new DisposableAction(() => list.ForEach(x => x.Dispose()));
			}, id));
			return AddLazyOperation(lazyLoadOperation, onEval, cmds);
		}

		internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval, IList<IDatabaseCommands> cmds)
		{
			pendingLazyOperations.Add(Tuple.Create(operation, cmds));
			var lazyValue = new Lazy<T>(() =>
			{
				ExecuteAllPendingLazyOperations();
				return (T) operation.Result;
			});
			if (onEval != null)
				onEvaluateLazy[operation] = result => onEval((T) result);

			return lazyValue;
		}

		/// <summary>
		/// Loads the specified entity with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Lazy<T> ILazySessionOperations.Load<T>(ValueType id)
		{
			return Lazily.Load(id, (Action<T>) null);
		}

		/// <summary>
		/// Loads the specified entity with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Lazy<T> ILazySessionOperations.Load<T>(ValueType id, Action<T> onEval)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Lazily.Load(documentKey, onEval);
		}

		Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Lazily.Load<T>(documentKeys, null);
		}

		Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids, Action<T[]> onEval)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Lazily.Load(documentKeys, onEval);
		}

		Lazy<T[]> ILazySessionOperations.Load<T>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Lazily.Load<T>(documentKeys, null);
		}

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
		{
			return new LazyMultiLoaderWithInclude<object>(this).Include(path);
		}

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
		{
			return new LazyMultiLoaderWithInclude<T>(this).Include(path);
		}
		
		/// <summary>
		/// Register to lazily load documents and include
		/// </summary>
		public Lazy<T[]> LazyLoadInternal<T>(string[] ids, string[] includes, Action<T[]> onEval)
		{
			var idsAndShards = ids.Select(id => new
			{
				id,
				shards = GetCommandsToOperateOn(new ShardRequestData
				{
					Keys = { id },
					EntityType = typeof(T),
				})
			})
			                      .GroupBy(x => x.shards, new DbCmdsListComparer());
			var cmds = idsAndShards.SelectMany(idAndShard => idAndShard.Key).Distinct().ToList();

			var multiLoadOperation = new MultiLoadOperation(this, () =>
			{
				var list = cmds.Select(cmd => cmd.DisableAllCaching()).ToList();
				return new DisposableAction(() => list.ForEach(disposable => disposable.Dispose()));
			}, ids, includes);
			var lazyOp = new LazyMultiLoadOperation<T>(multiLoadOperation, ids, includes);
			return AddLazyOperation(lazyOp, onEval, cmds);
		}

		public void ExecuteAllPendingLazyOperations()
		{
			if (pendingLazyOperations.Count == 0)
				return;

			try
			{
				IncrementRequestCount();
				while (ExecuteLazyOperationsSingleStep())
				{
					Thread.Sleep(100);
				}

				foreach (var pendingLazyOperation in pendingLazyOperations)
				{
					Action<object> value;
					if (onEvaluateLazy.TryGetValue(pendingLazyOperation.Item1, out value))
						value(pendingLazyOperation.Item1.Result);
				}
			}
			finally
			{
				pendingLazyOperations.Clear();
			}
		}

		private bool ExecuteLazyOperationsSingleStep()
		{
			var disposables = pendingLazyOperations.Select(x => x.Item1.EnterContext()).Where(x => x != null).ToList();
			try
			{
				var operationsPerShardGroup = pendingLazyOperations.GroupBy(x => x.Item2, new DbCmdsListComparer());

				foreach (var operationPerShard in operationsPerShardGroup)
				{
					var lazyOperations = operationPerShard.Select(x => x.Item1).ToArray();
					var requests = lazyOperations.Select(x => x.CreateRequest()).ToArray();
					var multiResponses = shardStrategy.ShardAccessStrategy.Apply(operationPerShard.Key, new ShardRequestData(),
					                                                             (commands, i) => commands.MultiGet(requests));

					var sb = new StringBuilder();
					foreach (var response in from shardResponses in multiResponses
					                         from getResponse in shardResponses
					                         where getResponse.RequestHasErrors()
					                         select getResponse)
						sb.AppendFormat("Got an error from server, status code: {0}{1}{2}", response.Status, Environment.NewLine,
						                response.Result)
						  .AppendLine();

					if (sb.Length > 0)
						throw new InvalidOperationException(sb.ToString());

					for (int i = 0; i < lazyOperations.Length; i++)
					{
						var copy = i;
						lazyOperations[i].HandleResponses(multiResponses.Select(x => x[copy]).ToArray(), shardStrategy);
						if (lazyOperations[i].RequiresRetry)
							return true;
					}
				}
				return false;
			}
			finally
			{
				disposables.ForEach(disposable => disposable.Dispose());
			}
		}

		#endregion

		#endregion

		#region Queries

		protected override IDocumentQuery<T> IDocumentQueryGeneratorQuery<T>(string indexName, bool isMapReduce = false)
		{
			return LuceneQuery<T>(indexName, isMapReduce);
		}

		protected override IAsyncDocumentQuery<T> IDocumentQueryGeneratorAsyncQuery<T>(string indexName, bool isMapReduce = false)
		{
			throw new NotSupportedException("The synchronous sharded document store doesn't support async operations");
		}

		public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexName = new TIndexCreator().IndexName;
			return LuceneQuery<T>(indexName);
		}

		public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
		{
			return new ShardedDocumentQuery<T>(this, GetShardsToOperateOn, shardStrategy, indexName, null, null,
			                                   listeners.QueryListeners, isMapReduce);
		}

		public IDocumentQuery<T> LuceneQuery<T>()
		{
			return LuceneQuery<T>(GetDynamicIndexName<T>());
		}

		#endregion

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		void IDocumentSession.SaveChanges()
		{
			using (EntityToJson.EntitiesToJsonCachingScope())
			{
				var data = PrepareForSaveChanges();
				if (data.Commands.Count == 0 && deferredCommandsByShard.Count == 0)
					return; // nothing to do here

				IncrementRequestCount();
				LogBatch(data);

				// split by shards
				var saveChangesPerShard = GetChangesToSavePerShard(data);

				// execute on all shards
				foreach (var shardAndObjects in saveChangesPerShard)
				{
					var shardId = shardAndObjects.Key;

					IDatabaseCommands databaseCommands;
					if (shardDbCommands.TryGetValue(shardId, out databaseCommands) == false)
						throw new InvalidOperationException(
							string.Format("ShardedDocumentStore cannot found a DatabaseCommands for shard id '{0}'.", shardId));

					var results = databaseCommands.Batch(shardAndObjects.Value.Commands);
					UpdateBatchResults(results, shardAndObjects.Value);
				}
			}
		}

		void ISyncAdvancedSessionOperation.Refresh<T>(T entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new InvalidOperationException("Cannot refresh a transient instance");
			IncrementRequestCount();

			var shardRequestData = new ShardRequestData
			{
				EntityType = typeof(T),
				Keys = { value.Key }
			};
			var dbCommands = GetCommandsToOperateOn(shardRequestData);

			var results = shardStrategy.ShardAccessStrategy.Apply(dbCommands, shardRequestData, (dbCmd, i) =>
			{
				var jsonDocument = dbCmd.Get(value.Key);
				if (jsonDocument == null)
					return false;

				value.Metadata = jsonDocument.Metadata;
				value.OriginalMetadata = (RavenJObject) jsonDocument.Metadata.CloneToken();
				value.ETag = jsonDocument.Etag;
				value.OriginalValue = jsonDocument.DataAsJson;
				var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
				foreach (
					var property in
						entity.GetType().GetProperties().Where(
							property => property.CanWrite && property.CanRead && property.GetIndexParameters().Length == 0))
				{
					property.SetValue(entity, property.GetValue(newEntity, null), null);
				}
				return true;
			});

			if (results.All(x => x == false))
			{
				throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");
			}
		}

		public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25)
		{
			IncrementRequestCount();
			var shards = GetCommandsToOperateOn(new ShardRequestData
			{
				EntityType = typeof(T),
				Keys = { keyPrefix }
			});
			var results = shardStrategy.ShardAccessStrategy.Apply(shards, new ShardRequestData
			{
				EntityType = typeof(T),
				Keys = { keyPrefix }
			}, (dbCmd, i) => dbCmd.StartsWith(keyPrefix, matches, start, pageSize));

			return results.SelectMany(x => x).Select(TrackEntity<T>)
			              .ToArray();
		}

		Lazy<T[]> ILazySessionOperations.LoadStartingWith<T>(string keyPrefix, string matches, int start, int pageSize)
		{
			IncrementRequestCount();
			var cmds = GetCommandsToOperateOn(new ShardRequestData
			{
				EntityType = typeof(T),
				Keys = { keyPrefix }
			});

			var lazyLoadOperation = new LazyStartsWithOperation<T>(keyPrefix, matches, start, pageSize, this);

			return AddLazyOperation<T[]>(lazyLoadOperation, null, cmds);
		}

		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Not supported for sharded session"); }
		}

		string ISyncAdvancedSessionOperation.GetDocumentUrl(object entity)
		{
			DocumentMetadata value;
			if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
				throw new ArgumentException("The entity is not part of the session");

			var shardId = value.Metadata.Value<string>(Constants.RavenShardId);
			IDatabaseCommands commands;
			if (shardDbCommands.TryGetValue(shardId, out commands) == false)
				throw new InvalidOperationException("Could not find matching shard for shard id: " + shardId);
			return commands.UrlFor(value.Key);
		}
	}
}

#endif
