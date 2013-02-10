//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing.Sorting;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.Version;
using Lucene.Net.Search.Vectorhighlight;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// 	This is a thread safe, single instance for a particular index.
	/// </summary>
	public abstract class Index : IDisposable
	{
		protected static readonly ILog logIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");
		protected static readonly ILog logQuerying = LogManager.GetLogger(typeof(Index).FullName + ".Querying");
		private readonly List<Document> currentlyIndexDocuments = new List<Document>();
		private Directory directory;
		protected readonly IndexDefinition indexDefinition;
		private volatile string waitReason;
		/// <summary>
		/// Note, this might be written to be multiple threads at the same time
		/// We don't actually care for exact timing, it is more about general feeling
		/// </summary>
		private DateTime? lastQueryTime;

		private readonly ConcurrentDictionary<string, IIndexExtension> indexExtensions =
			new ConcurrentDictionary<string, IIndexExtension>();

		internal readonly string name;

		private readonly AbstractViewGenerator viewGenerator;
		protected readonly WorkContext context;
		private readonly object writeLock = new object();
		private volatile bool disposed;
		private IndexWriter indexWriter;
		private SnapshotDeletionPolicy snapshotter;
		private readonly IndexSearcherHolder currentIndexSearcherHolder = new IndexSearcherHolder();

		private readonly ConcurrentQueue<IndexingPerformanceStats> indexingPerformanceStats = new ConcurrentQueue<IndexingPerformanceStats>();
		private readonly static StopAnalyzer stopAnalyzer = new StopAnalyzer(Version.LUCENE_30);

		public TimeSpan LastIndexingDuration { get; set; }
		public long TimePerDoc { get; set; }
		public Task CurrentMapIndexingTask { get; set; } 

		protected Index(Directory directory, string name, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator, WorkContext context)
		{
			if (directory == null) throw new ArgumentNullException("directory");
			if (name == null) throw new ArgumentNullException("name");
			if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");
			if (viewGenerator == null) throw new ArgumentNullException("viewGenerator");

			this.name = name;
			this.indexDefinition = indexDefinition;
			this.viewGenerator = viewGenerator;
			this.context = context;
			logIndexing.Debug("Creating index for {0}", name);
			this.directory = directory;

			RecreateSearcher();
		}

		[ImportMany]
		public OrderedPartCollection<AbstractAnalyzerGenerator> AnalyzerGenerators { get; set; }

		/// <summary>
		/// Whatever this is a map reduce index or not
		/// </summary>
		public abstract bool IsMapReduce { get; }

		public DateTime? LastQueryTime
		{
			get
			{
				return lastQueryTime;
			}
		}

		public DateTime LastIndexTime { get; set; }

		protected void AddindexingPerformanceStat(IndexingPerformanceStats stats)
		{
			indexingPerformanceStats.Enqueue(stats);
			if (indexingPerformanceStats.Count > 25)
				indexingPerformanceStats.TryDequeue(out stats);
		}

		public void Dispose()
		{
			try
			{
				// this is here so we can give good logs in the case of a long shutdown process
				if (Monitor.TryEnter(writeLock, 100) == false)
				{
					var localReason = waitReason;
					if (localReason != null)
						logIndexing.Warn("Waiting for {0} to complete before disposing of index {1}, that might take a while if the server is very busy",
						 localReason, name);

					Monitor.Enter(writeLock);
				}

				disposed = true;
				var task = CurrentMapIndexingTask;
				if (task != null)
				{
					try
					{
						task.Wait();
					}
					catch (Exception e)
					{
						logIndexing.Warn("Error while closing the index (could not wait for current indexing task)", e);
					}
				}

				foreach (var indexExtension in indexExtensions)
				{
					indexExtension.Value.Dispose();
				}

				if (currentIndexSearcherHolder != null)
				{
					var item = currentIndexSearcherHolder.SetIndexSearcher(null, wait: true);
					if (item.WaitOne(TimeSpan.FromSeconds(5)) == false)
					{
						logIndexing.Warn("After closing the index searching, we waited for 5 seconds for the searching to be done, but it wasn't. Continuing with normal shutdown anyway.");
					}
				}

				if (indexWriter != null)
				{
					var writer = indexWriter;
					indexWriter = null;

					try
					{
						writer.Analyzer.Close();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
					}

					try
					{
						writer.Dispose();
					}
					catch (Exception e)
					{
						logIndexing.ErrorException("Error when closing the index", e);
					}
				}

				try
				{
					directory.Dispose();
				}
				catch (Exception e)
				{
					logIndexing.ErrorException("Error when closing the directory", e);
				}
			}
			finally
			{
				Monitor.Exit(writeLock);
			}
		}

		public void Flush()
		{
			lock (writeLock)
			{
				if (disposed)
					return;
				if (indexWriter == null)
					return;

				try
				{
					waitReason = "Flush";
					indexWriter.Commit();
				}
				finally
				{
					waitReason = null;
				}
			}
		}

		public void MergeSegments()
		{
			lock (writeLock)
			{
				waitReason = "Merge / Optimize";
				try
				{
					logIndexing.Info("Starting merge of {0}", name);
					var sp = Stopwatch.StartNew();
					indexWriter.Optimize();
					logIndexing.Info("Done merging {0} - took {1}", name, sp.Elapsed);
				}
				finally
				{
					waitReason = null;
				}
			}
		}

		public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IndexingBatch batch, IStorageActionsAccessor actions, DateTime minimumTimestamp);


		protected virtual IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
		{
			return new IndexQueryResult
			{
				Score = score.Score,
				Key = document.Get(Constants.DocumentIdFieldName),
				Projection = fieldsToFetch.IsProjection ? CreateDocumentFromFields(document, fieldsToFetch) : null
			};
		}

		public static RavenJObject CreateDocumentFromFields(Document document, FieldsToFetch fieldsToFetch)
		{
			var documentFromFields = new RavenJObject();
			IEnumerable<string> fields = fieldsToFetch.Fields;

			if (fieldsToFetch.FetchAllStoredFields)
				fields = fields.Concat(document.GetFields().Select(x => x.Name));

			var q = fields
				.SelectMany(name => document.GetFields(name) ?? new Field[0])
				.Where(x => x != null)
				.Where(
					x =>
					x.Name.EndsWith("_IsArray") == false &&
					x.Name.EndsWith("_Range") == false &&
					x.Name.EndsWith("_ConvertToJson") == false)
				.Select(fld => CreateProperty(fld, document))
				.GroupBy(x => x.Key)
				.Select(g =>
				{
					if (g.Count() == 1 && document.GetField(g.Key + "_IsArray") == null)
					{
						return g.First();
					}
					var ravenJTokens = g.Select(x => x.Value).ToArray();
					return new KeyValuePair<string, RavenJToken>(g.Key, new RavenJArray((IEnumerable)ravenJTokens));
				});
			foreach (var keyValuePair in q)
			{
				documentFromFields.Add(keyValuePair.Key, keyValuePair.Value);
			}
			return documentFromFields;
		}

		private static KeyValuePair<string, RavenJToken> CreateProperty(Field fld, Document document)
		{
			if (fld.IsBinary)
				return new KeyValuePair<string, RavenJToken>(fld.Name, fld.GetBinaryValue());
			var stringValue = fld.StringValue;
			if (document.GetField(fld.Name + "_ConvertToJson") != null)
			{
				var val = RavenJToken.Parse(fld.StringValue) as RavenJObject;
				return new KeyValuePair<string, RavenJToken>(fld.Name, val);
			}
			if (stringValue == Constants.NullValue)
				stringValue = null;
			if (stringValue == Constants.EmptyString)
				stringValue = string.Empty;
			return new KeyValuePair<string, RavenJToken>(fld.Name, stringValue);
		}

		protected void Write(Func<IndexWriter, Analyzer, IndexingWorkStats, int> action)
		{
			if (disposed)
				throw new ObjectDisposedException("Index " + name + " has been disposed");
			LastIndexTime = SystemTime.UtcNow;
			lock (writeLock)
			{
				bool shouldRecreateSearcher;
				var toDispose = new List<Action>();
				Analyzer searchAnalyzer = null;
				try
				{
					waitReason = "Write";
					try
					{
						searchAnalyzer = CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose);
					}
					catch (Exception e)
					{
						context.AddError(name, "Creating Analyzer", e.ToString());
						throw;
					}

					if (indexWriter == null)
					{
						CreateIndexWriter();
					}

					var locker = directory.MakeLock("writing-to-index.lock");
					try
					{
						int changedDocs;
						var stats = new IndexingWorkStats();
						try
						{
							changedDocs = action(indexWriter, searchAnalyzer, stats);
							shouldRecreateSearcher = changedDocs > 0;
							foreach (var indexExtension in indexExtensions.Values)
							{
								indexExtension.OnDocumentsIndexed(currentlyIndexDocuments, searchAnalyzer);
							}
						}
						catch (Exception e)
						{
							context.AddError(name, null, e.ToString());
							throw;
						}

						if (changedDocs > 0)
						{
							UpdateIndexingStats(context, stats);
							WriteTempIndexToDiskIfNeeded(context);

							Flush(); // just make sure changes are flushed to disk
						}
					}
					finally
					{
						locker.Release();
					}
				}
				finally
				{
					currentlyIndexDocuments.Clear();
					if (searchAnalyzer != null)
						searchAnalyzer.Close();
					foreach (Action dispose in toDispose)
					{
						dispose();
					}
					waitReason = null;
					LastIndexTime = SystemTime.UtcNow;
				}
				if (shouldRecreateSearcher)
					RecreateSearcher();
			}
		}

		protected void UpdateIndexingStats(WorkContext context, IndexingWorkStats stats)
		{
			context.TransactionalStorage.Batch(accessor =>
			{
				switch (stats.Operation)
				{
					case IndexingWorkStats.Status.Map:
						accessor.Indexing.UpdateIndexingStats(name, stats);
						break;
					case IndexingWorkStats.Status.Reduce:
						accessor.Indexing.UpdateReduceStats(name, stats);
						break;
					case IndexingWorkStats.Status.Ignore:
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
			});
		}

		private void CreateIndexWriter()
		{
			snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
			indexWriter = new IndexWriter(directory, stopAnalyzer, snapshotter, IndexWriter.MaxFieldLength.UNLIMITED);
			using (indexWriter.MergeScheduler) { }
			indexWriter.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());

			// RavenDB already manages the memory for those, no need for Lucene to do this as well
			indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
			indexWriter.SetRAMBufferSizeMB(1024);
		}

		private void WriteTempIndexToDiskIfNeeded(WorkContext context)
		{
			if (context.Configuration.RunInMemory || !indexDefinition.IsTemp)
				return;

			var dir = indexWriter.Directory as RAMDirectory;
			if (dir == null ||
				dir.SizeInBytes() < context.Configuration.TempIndexInMemoryMaxBytes)
				return;

			indexWriter.Commit();
			var fsDir = context.IndexStorage.MakeRAMDirectoryPhysical(dir, indexDefinition.Name);
			directory = fsDir;

			indexWriter.Analyzer.Close();
			indexWriter.Dispose(true);

			CreateIndexWriter();
		}

		public RavenPerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose, bool forQuerying = false)
		{
			toDispose.Add(defaultAnalyzer.Close);

			string value;
			if (indexDefinition.Analyzers.TryGetValue(Constants.AllFields, out value))
			{
				defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value);
				toDispose.Add(defaultAnalyzer.Close);
			}
			var perFieldAnalyzerWrapper = new RavenPerFieldAnalyzerWrapper(defaultAnalyzer);
			foreach (var analyzer in indexDefinition.Analyzers)
			{
				Analyzer analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(analyzer.Key, analyzer.Value);
				toDispose.Add(analyzerInstance.Close);

				if (forQuerying)
				{
					var customAttributes = analyzerInstance.GetType().GetCustomAttributes(typeof(NotForQueryingAttribute), false);
					if (customAttributes.Length > 0)
						continue;
				}

				perFieldAnalyzerWrapper.AddAnalyzer(analyzer.Key, analyzerInstance);
			}
			StandardAnalyzer standardAnalyzer = null;
			KeywordAnalyzer keywordAnalyzer = null;
			foreach (var fieldIndexing in indexDefinition.Indexes)
			{
				switch (fieldIndexing.Value)
				{
					case FieldIndexing.NotAnalyzed:
						if (keywordAnalyzer == null)
						{
							keywordAnalyzer = new KeywordAnalyzer();
							toDispose.Add(keywordAnalyzer.Close);
						}
						perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, keywordAnalyzer);
						break;
					case FieldIndexing.Analyzed:
						if (indexDefinition.Analyzers.ContainsKey(fieldIndexing.Key))
							continue;
						if (standardAnalyzer == null)
						{
							standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
							toDispose.Add(standardAnalyzer.Close);
						}
						perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, standardAnalyzer);
						break;
				}
			}
			return perFieldAnalyzerWrapper;
		}

		protected IEnumerable<object> RobustEnumerationIndex(IEnumerator<object> input, IEnumerable<IndexingFunc> funcs,
															IStorageActionsAccessor actions, IndexingWorkStats stats)
		{
			return new RobustEnumerator(context, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => Interlocked.Increment(ref stats.IndexingAttempts),
				CancelMoveNext = () => Interlocked.Decrement(ref stats.IndexingAttempts),
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
										TryGetDocKey(o)),
						exception);

					stats.IndexingErrors++;
				}
			}.RobustEnumeration(input, funcs);
		}

		protected IEnumerable<object> RobustEnumerationReduce(IEnumerator<object> input, IndexingFunc func,
															IStorageActionsAccessor actions,
			IndexingWorkStats stats)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => Interlocked.Increment(ref stats.ReduceAttempts),
				CancelMoveNext = () => Interlocked.Decrement(ref stats.ReduceAttempts),
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
										TryGetDocKey(o)),
						exception);

					stats.ReduceErrors++;
				}
			}.RobustEnumeration(input, func);
		}

		// we don't care about tracking map/reduce stats here, since it is merely
		// an optimization step
		protected IEnumerable<object> RobustEnumerationReduceDuringMapPhase(IEnumerator<object> input, IndexingFunc func,
															IStorageActionsAccessor actions, WorkContext context)
		{
			// not strictly accurate, but if we get that many errors, probably an error anyway.
			return new RobustEnumerator(context, context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
			{
				BeforeMoveNext = () => { }, // don't care
				CancelMoveNext = () => { }, // don't care
				OnError = (exception, o) =>
				{
					context.AddError(name,
									TryGetDocKey(o),
									exception.Message
						);
					logIndexing.WarnException(
						String.Format("Failed to execute indexing function on {0} on {1}", name,
										TryGetDocKey(o)),
						exception);
				}
			}.RobustEnumeration(input, func);
		}

		public static string TryGetDocKey(object current)
		{
			var dic = current as DynamicJsonObject;
			if (dic == null)
				return null;
			object value = dic.GetValue(Constants.DocumentIdFieldName);
			if (value == null)
				return null;
			return value.ToString();
		}

		public abstract void Remove(string[] keys, WorkContext context);

		internal IDisposable GetSearcher(out IndexSearcher searcher)
		{
			return currentIndexSearcherHolder.GetSearcher(out searcher);
		}

		internal IDisposable GetSearcherAndTermsDocs(out IndexSearcher searcher, out RavenJObject[] termsDocs)
		{
			return currentIndexSearcherHolder.GetSearcherAndTermDocs(out searcher, out termsDocs);
		}

		private void RecreateSearcher()
		{
			if (indexWriter == null)
			{
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true), wait: false);
			}
			else
			{
				var indexReader = indexWriter.GetReader();
				currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader), wait: false);
			}
		}

		protected void AddDocumentToIndex(IndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer)
		{
			Analyzer newAnalyzer = AnalyzerGenerators.Aggregate(analyzer,
																(currentAnalyzer, generator) =>
																{
																	Analyzer generateAnalyzer =
																		generator.Value.GenerateAnalyzerForIndexing(name, luceneDoc,
																											currentAnalyzer);
																	if (generateAnalyzer != currentAnalyzer &&
																		currentAnalyzer != analyzer)
																		currentAnalyzer.Close();
																	return generateAnalyzer;
																});

			try
			{
				if (indexExtensions.Count > 0)
					currentlyIndexDocuments.Add(CloneDocument(luceneDoc));

				currentIndexWriter.AddDocument(luceneDoc, newAnalyzer);
			}
			finally
			{
				if (newAnalyzer != analyzer)
					newAnalyzer.Close();
			}
		}

		public void MarkQueried()
		{
			lastQueryTime = SystemTime.UtcNow;
		}

		public void MarkQueried(DateTime time)
		{
			lastQueryTime = time;
		}

		public IIndexExtension GetExtension(string indexExtensionKey)
		{
			IIndexExtension val;
			indexExtensions.TryGetValue(indexExtensionKey, out val);
			return val;
		}

		public IIndexExtension GetExtensionByPrefix(string indexExtensionKeyPrefix)
		{
			return indexExtensions.FirstOrDefault(x => x.Key.StartsWith(indexExtensionKeyPrefix)).Value;
		}

		public void SetExtension(string indexExtensionKey, IIndexExtension extension)
		{
			indexExtensions.TryAdd(indexExtensionKey, extension);
		}

		private static Document CloneDocument(Document luceneDoc)
		{
			var clonedDocument = new Document();
			foreach (AbstractField field in luceneDoc.GetFields())
			{
				var numericField = field as NumericField;
				if (numericField != null)
				{
					var clonedNumericField = new NumericField(numericField.Name,
															numericField.IsStored ? Field.Store.YES : Field.Store.NO,
															numericField.IsIndexed);
					var numericValue = numericField.NumericValue;
					if (numericValue is int)
					{
						clonedNumericField.SetIntValue((int)numericValue);
					}
					else if (numericValue is long)
					{
						clonedNumericField.SetLongValue((long)numericValue);
					}
					else if (numericValue is double)
					{
						clonedNumericField.SetDoubleValue((double)numericValue);
					}
					else if (numericValue is float)
					{
						clonedNumericField.SetFloatValue((float)numericValue);
					}
					clonedDocument.Add(clonedNumericField);
				}
				else
				{
					Field clonedField;
					if (field.IsBinary)
					{
						clonedField = new Field(field.Name, field.GetBinaryValue(),
												field.IsStored ? Field.Store.YES : Field.Store.NO);
					}
					else if (field.StringValue != null)
					{
						clonedField = new Field(field.Name, field.StringValue,
												field.IsStored ? Field.Store.YES : Field.Store.NO,
												field.IsIndexed ? Field.Index.ANALYZED_NO_NORMS : Field.Index.NOT_ANALYZED_NO_NORMS,
												field.IsTermVectorStored ? Field.TermVector.YES : Field.TermVector.NO);
					}
					else
					{
						//probably token stream, and we can't handle fields with token streams, so we skip this.
						continue;
					}
					clonedDocument.Add(clonedField);
				}
			}
			return clonedDocument;
		}

		protected void LogIndexedDocument(string key, Document luceneDoc)
		{
			if (logIndexing.IsDebugEnabled)
			{
				var fieldsForLogging = luceneDoc.GetFields().Cast<IFieldable>().Select(x => new
				{
					Name = x.Name,
					Value = x.IsBinary ? "<binary>" : x.StringValue,
					Indexed = x.IsIndexed,
					Stored = x.IsStored,
				});
				var sb = new StringBuilder();
				foreach (var fieldForLogging in fieldsForLogging)
				{
					sb.Append("\t").Append(fieldForLogging.Name)
						.Append(" ")
						.Append(fieldForLogging.Indexed ? "I" : "-")
						.Append(fieldForLogging.Stored ? "S" : "-")
						.Append(": ")
						.Append(fieldForLogging.Value)
						.AppendLine();
				}

				logIndexing.Debug("Indexing on {0} result in index {1} gave document: {2}", key, name,
								sb.ToString());
			}


		}


		#region Nested type: IndexQueryOperation

		internal class IndexQueryOperation
		{
			private readonly IndexQuery indexQuery;
			private readonly Index parent;
			private readonly Func<IndexQueryResult, bool> shouldIncludeInResults;
			private readonly HashSet<RavenJObject> alreadyReturned;
			private readonly FieldsToFetch fieldsToFetch;
			private readonly HashSet<string> documentsAlreadySeenInPreviousPage = new HashSet<string>();
			private readonly OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers;

			public IndexQueryOperation(Index parent, IndexQuery indexQuery, Func<IndexQueryResult, bool> shouldIncludeInResults,
										FieldsToFetch fieldsToFetch, OrderedPartCollection<AbstractIndexQueryTrigger> indexQueryTriggers)
			{
				this.parent = parent;
				this.indexQuery = indexQuery;
				this.shouldIncludeInResults = shouldIncludeInResults;
				this.fieldsToFetch = fieldsToFetch;
				this.indexQueryTriggers = indexQueryTriggers;

				if (fieldsToFetch.IsDistinctQuery)
					alreadyReturned = new HashSet<RavenJObject>(new RavenJTokenEqualityComparer());
			}

			public IEnumerable<RavenJObject> IndexEntries(Reference<int> totalResults)
			{
				parent.MarkQueried();
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed();
					IndexSearcher indexSearcher;
					RavenJObject[] termsDocs;
					using (parent.GetSearcherAndTermsDocs(out indexSearcher, out termsDocs))
					{
						var luceneQuery = ApplyIndexTriggers(GetLuceneQuery());

						TopDocs search = ExecuteQuery(indexSearcher, luceneQuery, indexQuery.Start, indexQuery.PageSize, indexQuery);
						totalResults.Value = search.TotalHits;

						for (int index = indexQuery.Start; index < search.ScoreDocs.Length; index++)
						{
							var scoreDoc = search.ScoreDocs[index];
							var ravenJObject = (RavenJObject)termsDocs[scoreDoc.Doc].CloneToken();
							foreach (var prop in ravenJObject.Where(x => x.Key.EndsWith("_Range")).ToArray())
							{
								ravenJObject.Remove(prop.Key);
							}
							yield return ravenJObject;
						}
					}
				}
			}

			public IEnumerable<IndexQueryResult> Query()
			{
				parent.MarkQueried();
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed();
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var luceneQuery = ApplyIndexTriggers(GetLuceneQuery());


						int start = indexQuery.Start;
						int pageSize = indexQuery.PageSize;
						int returnedResults = 0;
						int skippedResultsInCurrentLoop = 0;
						bool readAll;
						bool adjustStart = true;

						var recorder = new DuplicateDocumentRecorder(indexSearcher,
													  parent,
													  documentsAlreadySeenInPreviousPage,
													  alreadyReturned,
													  fieldsToFetch,
													  parent.IsMapReduce || fieldsToFetch.IsProjection);

						do
						{
							if (skippedResultsInCurrentLoop > 0)
							{
								start = start + pageSize - (start - indexQuery.Start); // need to "undo" the index adjustment
								// trying to guesstimate how many results we will need to read from the index
								// to get enough unique documents to match the page size
								pageSize = Math.Max(2, skippedResultsInCurrentLoop) * pageSize;
								skippedResultsInCurrentLoop = 0;
							}
							TopDocs search;
							int moreRequired;
							do
							{
								search = ExecuteQuery(indexSearcher, luceneQuery, start, pageSize, indexQuery);
								moreRequired = recorder.RecordResultsAlreadySeenForDistinctQuery(search, adjustStart, ref start);
								pageSize += moreRequired * 2;
							} while (moreRequired > 0);
							indexQuery.TotalSize.Value = search.TotalHits;
							adjustStart = false;

							FastVectorHighlighter highlighter = null;
							FieldQuery fieldQuery = null;

							if (indexQuery.HighlightedFields != null && indexQuery.HighlightedFields.Length > 0)
							{
								highlighter = new FastVectorHighlighter(
									FastVectorHighlighter.DEFAULT_PHRASE_HIGHLIGHT,
									FastVectorHighlighter.DEFAULT_FIELD_MATCH,
									new SimpleFragListBuilder(),
									new SimpleFragmentsBuilder(
										indexQuery.HighlighterPreTags != null && indexQuery.HighlighterPreTags.Any()
											? indexQuery.HighlighterPreTags
											: BaseFragmentsBuilder.COLORED_PRE_TAGS,
										indexQuery.HighlighterPostTags != null && indexQuery.HighlighterPostTags.Any()
											? indexQuery.HighlighterPostTags
											: BaseFragmentsBuilder.COLORED_POST_TAGS));

								fieldQuery = highlighter.GetFieldQuery(luceneQuery);
							}

							for (var i = start; (i - start) < pageSize && i < search.ScoreDocs.Length; i++)
							{
								var scoreDoc = search.ScoreDocs[i];
								var document = indexSearcher.Doc(scoreDoc.Doc);
								var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, scoreDoc);
								if (ShouldIncludeInResults(indexQueryResult) == false)
								{
									indexQuery.SkippedResults.Value++;
									skippedResultsInCurrentLoop++;
									continue;
								}

								if (highlighter != null)
								{
									var highlightings =
										from highlightedField in this.indexQuery.HighlightedFields
										select new
										{
											highlightedField.Field,
											highlightedField.FragmentsField,
											Fragments = highlighter.GetBestFragments(
												fieldQuery,
												indexSearcher.IndexReader,
												scoreDoc.Doc,
												highlightedField.Field,
												highlightedField.FragmentLength,
												highlightedField.FragmentCount)
										}
										into fieldHighlitings
										where fieldHighlitings.Fragments != null &&
											  fieldHighlitings.Fragments.Length > 0
										select fieldHighlitings;

									if (fieldsToFetch.IsProjection || parent.IsMapReduce)
									{
										foreach (var highlighting in highlightings)
											if (!string.IsNullOrEmpty(highlighting.FragmentsField))
												indexQueryResult.Projection[highlighting.FragmentsField]
													= new RavenJArray(highlighting.Fragments);
									} else
										indexQueryResult.Highligtings = highlightings
											.ToDictionary(x => x.Field, x => x.Fragments);
								}

								returnedResults++;
								yield return indexQueryResult;
								if (returnedResults == indexQuery.PageSize)
									yield break;
							}
							readAll = search.TotalHits == search.ScoreDocs.Length;
						} while (returnedResults < indexQuery.PageSize && readAll == false);
					}
				}
			}

			private Query ApplyIndexTriggers(Query luceneQuery)
			{
				luceneQuery = indexQueryTriggers.Aggregate(luceneQuery,
														   (current, indexQueryTrigger) =>
														   indexQueryTrigger.Value.ProcessQuery(parent.name, current, indexQuery));
				return luceneQuery;
			}

			public IEnumerable<IndexQueryResult> IntersectionQuery()
			{
				using (IndexStorage.EnsureInvariantCulture())
				{
					AssertQueryDoesNotContainFieldsThatAreNotIndexed();
					IndexSearcher indexSearcher;
					using (parent.GetSearcher(out indexSearcher))
					{
						var subQueries = indexQuery.Query.Split(new[] { Constants.IntersectSeparator }, StringSplitOptions.RemoveEmptyEntries);
						if (subQueries.Length <= 1)
							throw new InvalidOperationException("Invalid INTERSECT query, must have multiple intersect clauses.");

						//Not sure how to select the page size here??? The problem is that only docs in this search can be part 
						//of the final result because we're doing an intersection query (but we might exclude some of them)
						int pageSizeBestGuess = (indexQuery.Start + indexQuery.PageSize) * 2;
						int intersectMatches = 0, skippedResultsInCurrentLoop = 0;
						int previousBaseQueryMatches = 0, currentBaseQueryMatches = 0;

						var firstSubLuceneQuery = ApplyIndexTriggers(GetLuceneQuery(subQueries[0], indexQuery));

						//Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
						var search = ExecuteQuery(indexSearcher, firstSubLuceneQuery, 0, pageSizeBestGuess, indexQuery);
						currentBaseQueryMatches = search.ScoreDocs.Length;
						var intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);

						do
						{
							if (skippedResultsInCurrentLoop > 0)
							{
								// We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
								pageSizeBestGuess = pageSizeBestGuess * 2;

								search = ExecuteQuery(indexSearcher, firstSubLuceneQuery, 0, pageSizeBestGuess, indexQuery);
								previousBaseQueryMatches = currentBaseQueryMatches;
								currentBaseQueryMatches = search.ScoreDocs.Length;
								intersectionCollector = new IntersectionCollector(indexSearcher, search.ScoreDocs);
							}

							for (int i = 1; i < subQueries.Length; i++)
							{
								var luceneSubQuery = ApplyIndexTriggers(GetLuceneQuery(subQueries[i], indexQuery));
								indexSearcher.Search(luceneSubQuery, null, intersectionCollector);
							}

							var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
							intersectMatches = currentIntersectResults.Count;
							skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
						} while (intersectMatches < indexQuery.PageSize && //stop if we've got enough results to satisfy the pageSize
								 currentBaseQueryMatches < search.TotalHits && //stop if increasing the page size wouldn't make any difference
								 previousBaseQueryMatches < currentBaseQueryMatches); //stop if increasing the page size didn't result in any more "base query" results

						var intersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
						//It's hard to know what to do here, the TotalHits from the base search isn't really the TotalSize, 
						//because it's before the INTERSECTION has been applied, so only some of those results make it out.
						//Trying to give an accurate answer is going to be too costly, so we aren't going to try.
						indexQuery.TotalSize.Value = search.TotalHits;
						indexQuery.SkippedResults.Value = skippedResultsInCurrentLoop;

						//Using the final set of results in the intersectionCollector
						int returnedResults = 0;
						for (int i = indexQuery.Start; i < intersectResults.Count && (i - indexQuery.Start) < pageSizeBestGuess; i++)
						{
							Document document = indexSearcher.Doc(intersectResults[i].LuceneId);
							IndexQueryResult indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
							if (ShouldIncludeInResults(indexQueryResult) == false)
							{
								indexQuery.SkippedResults.Value++;
								skippedResultsInCurrentLoop++;
								continue;
							}
							returnedResults++;
							yield return indexQueryResult;
							if (returnedResults == indexQuery.PageSize)
								yield break;
						}
					}
				}
			}

			private bool ShouldIncludeInResults(IndexQueryResult indexQueryResult)
			{
				if (shouldIncludeInResults(indexQueryResult) == false)
					return false;
				if (documentsAlreadySeenInPreviousPage.Contains(indexQueryResult.Key))
					return false;
				if (fieldsToFetch.IsDistinctQuery && alreadyReturned.Add(indexQueryResult.Projection) == false)
					return false;
				return true;
			}

			private void RecordResultsAlreadySeenForDistinctQuery(IndexSearcher indexSearcher, TopDocs search, int start, int pageSize)
			{
				var min = Math.Min(start, search.TotalHits);

				// we are paging, we need to check that we don't have duplicates in the previous page
				// see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e
				if (parent.IsMapReduce == false && fieldsToFetch.IsProjection == false && start - pageSize >= 0 && start < search.TotalHits)
				{
					for (int i = start - pageSize; i < min; i++)
					{
						var document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
						documentsAlreadySeenInPreviousPage.Add(document.Get(Constants.DocumentIdFieldName));
					}
				}

				if (fieldsToFetch.IsDistinctQuery == false)
					return;

				// add results that were already there in previous pages
				for (int i = 0; i < min; i++)
				{
					Document document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
					var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
					alreadyReturned.Add(indexQueryResult.Projection);
				}
			}

			private void AssertQueryDoesNotContainFieldsThatAreNotIndexed()
			{
				if (string.IsNullOrWhiteSpace(indexQuery.Query))
					return;
				HashSet<string> hashSet = SimpleQueryParser.GetFields(indexQuery);
				foreach (string field in hashSet)
				{
					string f = field;
					if (f.EndsWith("_Range"))
					{
						f = f.Substring(0, f.Length - "_Range".Length);
					}
					if (parent.viewGenerator.ContainsField(f) == false &&
						parent.viewGenerator.ContainsField("_") == false) // the catch all field name means that we have dynamic fields names
						throw new ArgumentException("The field '" + f + "' is not indexed, cannot query on fields that are not indexed");
				}

				if (indexQuery.SortedFields == null)
					return;

				foreach (SortedField field in indexQuery.SortedFields)
				{
					string f = field.Field;
					if (f == Constants.TemporaryScoreValue)
						continue;
					if (f.EndsWith("_Range"))
					{
						f = f.Substring(0, f.Length - "_Range".Length);
					}
					if (f.StartsWith(Constants.RandomFieldName))
						continue;
					if (parent.viewGenerator.ContainsField(f) == false && f != Constants.DistanceFieldName
						&& parent.viewGenerator.ContainsField("_") == false)// the catch all field name means that we have dynamic fields names
						throw new ArgumentException("The field '" + f + "' is not indexed, cannot sort on fields that are not indexed");
				}
			}

			public Query GetLuceneQuery()
			{
				var q = GetLuceneQuery(indexQuery.Query, indexQuery);
				var spatialIndexQuery = indexQuery as SpatialIndexQuery;
				if (spatialIndexQuery != null)
				{
					var spatialStrategy = parent.viewGenerator.GetStrategyForField(spatialIndexQuery.SpatialFieldName);
					var dq = SpatialIndex.MakeQuery(q, spatialStrategy, spatialIndexQuery.QueryShape, spatialIndexQuery.SpatialRelation, spatialIndexQuery.DistanceErrorPercentage);
					if (q is MatchAllDocsQuery) return dq;

					var bq = new BooleanQuery { { q, Occur.MUST }, { dq, Occur.MUST } };
					return bq;
				}
				return q;
			}

			private Query GetLuceneQuery(string query, IndexQuery indexQuery)
			{
				Query luceneQuery;
				if (String.IsNullOrEmpty(query))
				{
					logQuerying.Debug("Issuing query on index {0} for all documents", parent.name);
					luceneQuery = new MatchAllDocsQuery();
				}
				else
				{
					logQuerying.Debug("Issuing query on index {0} for: {1}", parent.name, query);
					var toDispose = new List<Action>();
					RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
					try
					{
						searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
						searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
						{
							Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.name, indexQuery.Query, currentAnalyzer);
							if (newAnalyzer != currentAnalyzer)
							{
								DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
							}
							return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
						});
						luceneQuery = QueryBuilder.BuildQuery(query, indexQuery, searchAnalyzer);
					}
					finally
					{
						DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
					}
				}
				return luceneQuery;
			}

			private static void DisposeAnalyzerAndFriends(List<Action> toDispose, RavenPerFieldAnalyzerWrapper analyzer)
			{
				if (analyzer != null)
					analyzer.Close();
				foreach (Action dispose in toDispose)
				{
					dispose();
				}
				toDispose.Clear();
			}

			private TopDocs ExecuteQuery(IndexSearcher indexSearcher, Query luceneQuery, int start, int pageSize,
										IndexQuery indexQuery)
			{
				var sort = indexQuery.GetSort(parent.indexDefinition);

				if (pageSize == Int32.MaxValue) // we want all docs
				{
					var gatherAllCollector = new GatherAllCollector();
					indexSearcher.Search(luceneQuery, gatherAllCollector);
					return gatherAllCollector.ToTopDocs();
				}
				var minPageSize = Math.Max(pageSize + start, 1);

				// NOTE: We get Start + Pagesize results back so we have something to page on
				if (sort != null)
				{
					try
					{
						//indexSearcher.SetDefaultFieldSortScoring (sort.GetSort().Contains(SortField.FIELD_SCORE), false);
						indexSearcher.SetDefaultFieldSortScoring(true, false);
						var ret = indexSearcher.Search(luceneQuery, null, minPageSize, sort);
						return ret;
					}
					finally
					{
						indexSearcher.SetDefaultFieldSortScoring(false, false);
					}
				}
				return indexSearcher.Search(luceneQuery, null, minPageSize);
			}
		}

		#endregion

		public class DuplicateDocumentRecorder
		{
			private int min = -1;
			private readonly bool isProjectionOrMapReduce;
			private readonly Searchable indexSearcher;
			private readonly Index parent;
			private int alreadyScannedPositions;
			private readonly HashSet<string> documentsAlreadySeenInPreviousPage;
			private readonly HashSet<RavenJObject> alreadyReturned;
			private readonly FieldsToFetch fieldsToFetch;
			private int itemsSkipped;

			public DuplicateDocumentRecorder(Searchable indexSearcher,
				Index parent,
				HashSet<string> documentsAlreadySeenInPreviousPage,
				HashSet<RavenJObject> alreadyReturned,
				FieldsToFetch fieldsToFetch,
				bool isProjectionOrMapReduce)
			{
				this.indexSearcher = indexSearcher;
				this.parent = parent;
				this.isProjectionOrMapReduce = isProjectionOrMapReduce;
				this.alreadyReturned = alreadyReturned;
				this.fieldsToFetch = fieldsToFetch;
				this.documentsAlreadySeenInPreviousPage = documentsAlreadySeenInPreviousPage;
			}


			public int RecordResultsAlreadySeenForDistinctQuery(TopDocs search, bool adjustStart, ref int start)
			{
				if (min == -1)
					min = start;
				min = Math.Min(min, search.TotalHits);

				// we are paging, we need to check that we don't have duplicates in the previous pages
				// see here for details: http://groups.google.com/group/ravendb/browse_frm/thread/d71c44aa9e2a7c6e
				if (isProjectionOrMapReduce == false)
				{
					for (int i = alreadyScannedPositions; i < min; i++)
					{
						if (i >= search.ScoreDocs.Length)
						{
							alreadyScannedPositions = i;
							var pageSizeIncreaseSize = min - search.ScoreDocs.Length;
							return pageSizeIncreaseSize;
						}
						var document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
						var id = document.Get(Constants.DocumentIdFieldName);
						if (documentsAlreadySeenInPreviousPage.Add(id) == false)
						{
							// already seen this, need to expand the range we are scanning because the user
							// didn't take this into account
							min = Math.Min(min + 1, search.TotalHits);
							itemsSkipped++;
						}
					}
					alreadyScannedPositions = min;
				}
				if (adjustStart)
				{
					start += itemsSkipped;
				}

				if (fieldsToFetch.IsDistinctQuery == false)
					return 0;

				// add results that were already there in previous pages
				for (int i = 0; i < min; i++)
				{
					Document document = indexSearcher.Doc(search.ScoreDocs[i].Doc);
					var indexQueryResult = parent.RetrieveDocument(document, fieldsToFetch, search.ScoreDocs[i]);
					alreadyReturned.Add(indexQueryResult.Projection);
				}
				return 0;
			}
		}

		public IndexingPerformanceStats[] GetIndexingPerformance()
		{
			return indexingPerformanceStats.ToArray();
		}

		public void Backup(string backupDirectory, string path, string incrementalTag)
		{
			try
			{
				var existingFiles = new List<string>();
				if (incrementalTag != null)
					backupDirectory = Path.Combine(backupDirectory, incrementalTag);
				
				var allFilesPath = Path.Combine(backupDirectory, MonoHttpUtility.UrlEncode(name) + ".all-existing-index-files");
				var saveToFolder = Path.Combine(backupDirectory, "Indexes", MonoHttpUtility.UrlEncode(name));
				System.IO.Directory.CreateDirectory(saveToFolder);
				if (File.Exists(allFilesPath))
				{
					existingFiles.AddRange(File.ReadLines(allFilesPath));
				}

				using (var allFilesWriter = File.Exists(allFilesPath) ? File.AppendText(allFilesPath) : File.CreateText(allFilesPath))
				using (var neededFilesWriter = File.CreateText(Path.Combine(saveToFolder, "index-files.required-for-index-restore")))
				{
					// this is called for the side effect of creating the snapshotter and the writer
					// we explicitly handle the backup outside of the write, to allow concurrent indexing
					Write((writer, analyzer, stats) =>
					{
						// however, we copy the current segments.gen & index.version to make 
						// sure that we get the _at the time_ of the write. 
						foreach (var fileName in new[] { "segments.gen", "index.version" })
						{
							var fullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(name), fileName);
							File.Copy(fullPath, Path.Combine(saveToFolder, fileName));
							allFilesWriter.WriteLine(fileName);
						}
						return 0;
					});

					var commit = snapshotter.Snapshot();

					foreach (var fileName in commit.FileNames)
					{
						var fullPath = Path.Combine(path, MonoHttpUtility.UrlEncode(name), fileName);

						if (".lock".Equals(Path.GetExtension(fullPath), StringComparison.InvariantCultureIgnoreCase))
							continue;

						if (File.Exists(fullPath) == false)
							continue;

						if (existingFiles.Contains(fileName) == false)
						{
							File.Copy(fullPath, Path.Combine(saveToFolder, fileName));
							allFilesWriter.WriteLine(fileName);
						}
						neededFilesWriter.WriteLine(fileName);
					}

					allFilesWriter.Flush();
					neededFilesWriter.Flush();
				}
			}
			finally
			{
				if (snapshotter != null)
					snapshotter.Release();
			}
		}

		protected object LoadDocument(string key)
		{
			var jsonDocument = context.Database.Get(key, null);
			if (jsonDocument == null)
				return new DynamicNullObject();
			return new DynamicJsonObject(jsonDocument.ToJson());
		}
	}
}
