//-----------------------------------------------------------------------
// <copyright file="MapReduceIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Data;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{

	public class MapReduceIndex : Index
	{
		readonly JsonSerializer jsonSerializer;

		public MapReduceIndex(Directory directory, string name, IndexDefinition indexDefinition,
							  AbstractViewGenerator viewGenerator, WorkContext context)
			: base(directory, name, indexDefinition, viewGenerator, context)
		{
			jsonSerializer = new JsonSerializer();
			foreach (var jsonConverter in Default.Converters)
			{
				jsonSerializer.Converters.Add(jsonConverter);
			}
		}

		public override bool IsMapReduce
		{
			get { return true; }
		}

		private class MapResultItem
		{
			public string DocId;
			public RavenJObject Data;
			public string ReduceKey;
		}

		public override void IndexDocuments(
			AbstractViewGenerator viewGenerator,
			IndexingBatch batch,
			IStorageActionsAccessor actions,
			DateTime minimumTimestamp)
		{
			var count = 0;
			var sourceCount = 0;
			var sw = Stopwatch.StartNew();
			var start = SystemTime.UtcNow;
			var changed = new HashSet<ReduceKeyAndBucket>();
			var documentsWrapped = batch.Docs.Select(doc =>
			{
				sourceCount++;
				var documentId = doc.__document_id;
				actions.MapReduce.DeleteMappedResultsForDocumentId((string)documentId, name, changed);
				return doc;
			})
				.Where(x => x is FilteredDocument == false);
			var items = new List<MapResultItem>();
			var stats = new IndexingWorkStats();
			var allReferencedDocs = new ConcurrentQueue<IDictionary<string, HashSet<string>>>();
			using (CurrentIndexingScope.Current = new CurrentIndexingScope(LoadDocument, allReferencedDocs.Enqueue))
			{
				var mapResults = RobustEnumerationIndex(
						documentsWrapped.GetEnumerator(), 
						viewGenerator.MapDefinitions, 
						actions, 
						stats)
					.ToList();
				actions.MapReduce.UpdateRemovedMapReduceStats(name, changed);

				foreach (var mappedResultFromDocument in mapResults.GroupBy(GetDocumentId))
				{
					var dynamicResults = mappedResultFromDocument.Select(x => (object)new DynamicJsonObject(RavenJObject.FromObject(x, jsonSerializer))).ToList();
					foreach (
						var doc in
							RobustEnumerationReduceDuringMapPhase(dynamicResults.GetEnumerator(), viewGenerator.ReduceDefinition, actions, context))
					{
						count++;

						var reduceValue = viewGenerator.GroupByExtraction(doc);
						if (reduceValue == null)
						{
							logIndexing.Debug("Field {0} is used as the reduce key and cannot be null, skipping document {1}",
											  viewGenerator.GroupByExtraction, mappedResultFromDocument.Key);
							continue;
						}
						var reduceKey = ReduceKeyToString(reduceValue);
						var docId = mappedResultFromDocument.Key.ToString();

						var data = GetMappedData(doc);

						items.Add(new MapResultItem
						{
							Data = data,
							DocId = docId,
							ReduceKey = reduceKey
						});

						changed.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(docId), reduceKey));
					}
				}
			}

			IDictionary<string, HashSet<string>> result;
			while (allReferencedDocs.TryDequeue(out result))
			{
				foreach (var referencedDocument in result)
				{
					actions.Indexing.UpdateDocumentReferences(name, referencedDocument.Key, referencedDocument.Value);
					actions.General.MaybePulseTransaction();
				}
			}

			foreach (var mapResultItem in items)
			{
				actions.MapReduce.PutMappedResult(name, mapResultItem.DocId, mapResultItem.ReduceKey, mapResultItem.Data);
				actions.General.MaybePulseTransaction();
			}

			UpdateIndexingStats(context, stats);
			actions.MapReduce.ScheduleReductions(name, 0, changed);
			AddindexingPerformanceStat(new IndexingPerformanceStats
			{
				OutputCount = count,
				InputCount = sourceCount,
				Operation = "Map",
				Duration = sw.Elapsed,
				Started = start
			});
			logIndexing.Debug("Mapped {0} documents for {1}", count, name);
		}

		private RavenJObject GetMappedData(object doc)
		{
			if (doc is IDynamicJsonObject)
				return ((IDynamicJsonObject)doc).Inner;

			var ravenJTokenWriter = new RavenJTokenWriter();
			jsonSerializer.Serialize(ravenJTokenWriter, doc);
			return (RavenJObject)ravenJTokenWriter.Token;
		}

		private static readonly ConcurrentDictionary<Type, Func<object, object>> documentIdFetcherCache =
			new ConcurrentDictionary<Type, Func<object, object>>();

		private static object GetDocumentId(object doc)
		{
			var docIdFetcher = documentIdFetcherCache.GetOrAdd(doc.GetType(), type =>
			{
				// document may be DynamicJsonObject if we are using compiled views
				if (typeof(DynamicJsonObject) == type)
				{
					return i => ((dynamic)i).__document_id;
				}
				var docIdProp = TypeDescriptor.GetProperties(doc).Find(Constants.DocumentIdFieldName, false);
				return docIdProp.GetValue;
			});
			if (docIdFetcher == null)
				throw new InvalidOperationException("Could not create document id fetcher for this document");
			var documentId = docIdFetcher(doc);
			if (documentId == null || documentId is DynamicNullObject)
				throw new InvalidOperationException("Could not getdocument id fetcher for this document");

			return documentId;
		}

		internal static string ReduceKeyToString(object reduceValue)
		{
			if (reduceValue is string || reduceValue is ValueType)
				return reduceValue.ToString();
			var dynamicJsonObject = reduceValue as IDynamicJsonObject;
			if (dynamicJsonObject != null)
				return dynamicJsonObject.Inner.ToString(Formatting.None);
			return RavenJToken.FromObject(reduceValue).ToString(Formatting.None);
		}

		protected override IndexQueryResult RetrieveDocument(Document document, FieldsToFetch fieldsToFetch, ScoreDoc score)
		{
			if (fieldsToFetch.IsProjection == false)
				fieldsToFetch = fieldsToFetch.CloneWith(document.GetFields().Select(x => x.Name).ToArray());
			fieldsToFetch.EnsureHasField(Constants.ReduceKeyFieldName);
			return base.RetrieveDocument(document, fieldsToFetch, score);
		}

		public override void Remove(string[] keys, WorkContext context)
		{
			context.TransactionalStorage.Batch(actions =>
			{
				var reduceKeyAndBuckets = new HashSet<ReduceKeyAndBucket>();
				foreach (var key in keys)
				{
					actions.MapReduce.DeleteMappedResultsForDocumentId(key, name, reduceKeyAndBuckets);
				}

				actions.MapReduce.UpdateRemovedMapReduceStats(name, reduceKeyAndBuckets);
				actions.MapReduce.ScheduleReductions(name, 0, reduceKeyAndBuckets);
			});
			Write((writer, analyzer, stats) =>
			{
				stats.Operation = IndexingWorkStats.Status.Ignore;
				logIndexing.Debug(() => string.Format("Deleting ({0}) from {1}", string.Join(", ", keys), name));
				writer.DeleteDocuments(keys.Select(k => new Term(Constants.ReduceKeyFieldName, k.ToLowerInvariant())).ToArray());
				return keys.Length;
			});
		}

		public class ReduceDocuments
		{
			private readonly MapReduceIndex parent;
			private readonly string name;
			readonly AnonymousObjectToLuceneDocumentConverter anonymousObjectToLuceneDocumentConverter;
			private readonly Document luceneDoc = new Document();
			private readonly Field reduceKeyField = new Field(Constants.ReduceKeyFieldName, "dummy",
													 Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
			private PropertyDescriptorCollection properties = null;
			private readonly List<AbstractIndexUpdateTriggerBatcher> batchers;

			public ReduceDocuments(
				MapReduceIndex parent,
				AbstractViewGenerator viewGenerator,
				IEnumerable<IGrouping<int, object>> mappedResultsByBucket,
				int level,
				WorkContext context,
				IStorageActionsAccessor actions,
				HashSet<string> reduceKeys)
			{
				this.parent = parent;
				name = this.parent.name;
				ViewGenerator = viewGenerator;
				MappedResultsByBucket = mappedResultsByBucket;
				Level = level;
				Context = context;
				Actions = actions;
				ReduceKeys = reduceKeys;

				anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(this.parent.indexDefinition);

				if (Level == 2)
				{
					batchers = Context.IndexUpdateTriggers.Select(x => x.CreateBatcher(name))
								.Where(x => x != null)
								.ToList();
				}
			}

			public AbstractViewGenerator ViewGenerator { get; private set; }
			public IEnumerable<IGrouping<int, object>> MappedResultsByBucket { get; private set; }
			public int Level { get; private set; }
			public WorkContext Context { get; private set; }
			public IStorageActionsAccessor Actions { get; private set; }
			public HashSet<string> ReduceKeys { get; private set; }

			private string ExtractReduceKey(AbstractViewGenerator viewGenerator, object doc)
			{
				try
				{
					object reduceKey = viewGenerator.GroupByExtraction(doc);
					if (reduceKey == null)
					{
						throw new InvalidOperationException("Could not find reduce key for " + name + " in the result: " + doc);
					}
					return ReduceKeyToString(reduceKey);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not extract reduce key from reduce result!", e);
				}
			}

			private IEnumerable<AbstractField> GetFields(object doc, out float boost)
			{
				boost = 1;
				var boostedValue = doc as BoostedValue;
				if (boostedValue != null)
				{
					doc = boostedValue.Value;
					boost = boostedValue.Boost;
				}
				IEnumerable<AbstractField> fields;
				if (doc is IDynamicJsonObject)
				{

					fields = anonymousObjectToLuceneDocumentConverter.Index(((IDynamicJsonObject)doc).Inner, Field.Store.YES);
				}
				else
				{
					properties = properties ?? TypeDescriptor.GetProperties(doc);
					fields = anonymousObjectToLuceneDocumentConverter.Index(doc, properties, Field.Store.YES);
				}
				if (Math.Abs(boost - 1) > float.Epsilon)
				{
					var abstractFields = fields.ToList();
					foreach (var abstractField in abstractFields)
					{
						abstractField.OmitNorms = false;
					}
					return abstractFields;
				}
				return fields;
			}

			private static RavenJObject ToJsonDocument(object doc)
			{
				var boostedValue = doc as BoostedValue;
				if (boostedValue != null)
				{
					doc = boostedValue.Value;
				}
				var dynamicJsonObject = doc as IDynamicJsonObject;
				if (dynamicJsonObject != null)
				{
					return dynamicJsonObject.Inner;
				}
				var ravenJObject = doc as RavenJObject;
				if (ravenJObject != null)
					return ravenJObject;
				return RavenJObject.FromObject(doc);
			}

			public void ExecuteReduction()
			{
				var count = 0;
				var sourceCount = 0;
				var sw = Stopwatch.StartNew();
				var start = SystemTime.UtcNow;

				parent.Write((indexWriter, analyzer, stats) =>
				{
					stats.Operation = IndexingWorkStats.Status.Reduce;
					try
					{
						if (Level == 2)
						{
							RemoveExistingReduceKeysFromIndex(indexWriter);
						}
						foreach (var mappedResults in MappedResultsByBucket)
						{
							var input = mappedResults.Select(x =>
							{
								sourceCount++;
								return x;
							});
							foreach (var doc in parent.RobustEnumerationReduce(input.GetEnumerator(), ViewGenerator.ReduceDefinition, Actions, stats))
							{
								count++;
								string reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);

								switch (Level)
								{
									case 0:
									case 1:
										Actions.MapReduce.PutReducedResult(name, reduceKeyAsString, Level + 1, mappedResults.Key, mappedResults.Key / 1024, ToJsonDocument(doc));
										Actions.General.MaybePulseTransaction();
										break;
									case 2:
										WriteDocumentToIndex(doc, indexWriter, analyzer);
										break;
									default:
										throw new InvalidOperationException("Unknown level: " + Level);
								}
								stats.ReduceSuccesses++;
							}
						}
					}
					catch (Exception e)
					{
						if (Level == 2)
						{
							batchers.ApplyAndIgnoreAllErrors(
								ex =>
								{
									logIndexing.WarnException("Failed to notify index update trigger batcher about an error", ex);
									Context.AddError(name, null, ex.Message);
								},
								x => x.AnErrorOccured(e));
						}
						throw;
					}
					finally
					{
						if (Level == 2)
						{
							batchers.ApplyAndIgnoreAllErrors(
								e =>
								{
									logIndexing.WarnException("Failed to dispose on index update trigger", e);
									Context.AddError(name, null, e.Message);
								},
								x => x.Dispose());
						}
					}
					return count + ReduceKeys.Count;
				});
				parent.AddindexingPerformanceStat(new IndexingPerformanceStats
				{
					OutputCount = count,
					InputCount = sourceCount,
					Duration = sw.Elapsed,
					Operation = "Reduce Level " + Level,
					Started = start
				});
				logIndexing.Debug(() => string.Format("Reduce resulted in {0} entries for {1} for reduce keys: {2}", count, name, string.Join(", ", ReduceKeys)));
			}

			private void WriteDocumentToIndex(object doc, IndexWriter indexWriter, Analyzer analyzer)
			{
				float boost;
				var fields = GetFields(doc, out boost).ToList();

				string reduceKeyAsString = ExtractReduceKey(ViewGenerator, doc);
				reduceKeyField.SetValue(reduceKeyAsString);

				luceneDoc.GetFields().Clear();
				luceneDoc.Boost = boost;
				luceneDoc.Add(reduceKeyField);
				foreach (var field in fields)
				{
					luceneDoc.Add(field);
				}

				if (Level == 2)
				{
					batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryCreated trigger for index '{0}', key: '{1}'",
											  name, reduceKeyAsString),
								exception);
							Context.AddError(name, reduceKeyAsString, exception.Message);
						},
						trigger => trigger.OnIndexEntryCreated(reduceKeyAsString, luceneDoc));
				}

				parent.LogIndexedDocument(reduceKeyAsString, luceneDoc);

				parent.AddDocumentToIndex(indexWriter, luceneDoc, analyzer);
			}

			private void RemoveExistingReduceKeysFromIndex(IndexWriter indexWriter)
			{
				foreach (var reduceKey in ReduceKeys)
				{
					var entryKey = reduceKey;
					indexWriter.DeleteDocuments(new Term(Constants.ReduceKeyFieldName, entryKey));
					batchers.ApplyAndIgnoreAllErrors(
						exception =>
						{
							logIndexing.WarnException(
								string.Format("Error when executed OnIndexEntryDeleted trigger for index '{0}', key: '{1}'",
											  name, entryKey),
								exception);
							Context.AddError(name, entryKey, exception.Message);
						},
						trigger => trigger.OnIndexEntryDeleted(entryKey));
				}
			}
		}
	}
}