﻿#if !SILVERLIGHT
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Listeners;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	public class DocumentQuery<T> : AbstractDocumentQuery<T, DocumentQuery<T>>, IDocumentQuery<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery{T}"/> class.
		/// </summary>
		public DocumentQuery(InMemoryDocumentSessionOperations session
#if !SILVERLIGHT
			, IDatabaseCommands databaseCommands
#endif 
			, IAsyncDatabaseCommands asyncDatabaseCommands, string indexName, string[] fieldsToFetch, string[] projectionFields, IDocumentQueryListener[] queryListeners, bool isMapReduce)
			: base(session
#if !SILVERLIGHT
			, databaseCommands
#endif
			, asyncDatabaseCommands, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery{T}"/> class.
		/// </summary>
		public DocumentQuery(DocumentQuery<T> other)
			: base(other)
		{
			
		}

		/// <summary>
		/// Selects the projection fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		public IDocumentQuery<TProjection> SelectFields<TProjection>()
		{
			var props = typeof (TProjection).GetProperties().Select(x => x.Name).ToArray();
			return SelectFields<TProjection>(props, props);
		}

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		public IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
		{
			return SelectFields<TProjection>(fields, fields);
		}

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		public virtual IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections)
		{
			var documentQuery = new DocumentQuery<TProjection>(theSession,
#if !SILVERLIGHT
															   theDatabaseCommands,
#endif
															   theAsyncDatabaseCommands,
															   indexName, 
															   fields,
															   projections,
															   queryListeners, 
															   isMapReduce)
			{
				pageSize = pageSize,
				queryText = new StringBuilder(queryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				cutoffEtag = cutoffEtag,
				queryStats = queryStats,
				theWaitForNonStaleResults = theWaitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				groupByFields = groupByFields,
				aggregationOp = aggregationOp,
				negate = negate,
				transformResultsFunc = transformResultsFunc,
				includes = new HashSet<string>(includes),
				isSpatialQuery = isSpatialQuery,
				spatialFieldName = spatialFieldName,
				queryShape = queryShape,
				spatialRelation = spatialRelation,
				distanceErrorPct = distanceErrorPct,
				rootTypes = {typeof(T)},
				defaultField = defaultField,
				beforeQueryExecutionAction = beforeQueryExecutionAction,
				afterQueryExecutedCallback = afterQueryExecutedCallback,
				highlightedFields = new List<HighlightedField>(highlightedFields),
				highlighterPreTags = highlighterPreTags,
				highlighterPostTags = highlighterPostTags
			};
			return documentQuery;
		}

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			WaitForNonStaleResults(waitTimeout);
			return this;
		}

		/// <summary>
		/// Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder(string fieldName, bool descending)
		{
			AddOrder(fieldName, descending);
			return this;
		}

		/// <summary>
		///   Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "descending">if set to <c>true</c> [descending].</param>
		public IDocumentQuery<T> AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending)
		{
			AddOrder(GetMemberQueryPath(propertySelector.Body), descending);
			return this;
		}

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering()
		{
			RandomOrdering();
			return this;
		}

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering(string seed)
		{
			RandomOrdering(seed);
			return this;
		}

		/// <summary>
		/// Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		/// <param name="fieldType">the type of the field to be sorted.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder(string fieldName, bool descending, Type fieldType)
		{
			AddOrder(fieldName, descending, fieldType);
			return this;
		}

		/// <summary>
		/// Simplified method for opening a new clause within the query
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OpenSubclause()
		{
			OpenSubclause();
			return this;
		}

		/// <summary>
		/// Simplified method for closing a clause within the query
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.CloseSubclause()
		{
			CloseSubclause();
			return this;
		}

		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Search(string fieldName, string searchTerms)
		{
			Search(fieldName, searchTerms);
			return this;
		}

		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		public IDocumentQuery<T> Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms)
		{
			Search(GetMemberQueryPath(propertySelector.Body), searchTerms);
			return this;
		}

		///<summary>
		/// Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		/// <remarks>
		/// This is only valid on dynamic indexes queries
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
		{
			GroupBy(aggregationOperation, fieldsToGroupBy);
			return this;
		}

		///<summary>
		///  Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		///<remarks>
		///  This is only valid on dynamic indexes queries
		///</remarks>
		public IDocumentQuery<T> GroupBy<TValue>(AggregationOperation aggregationOperation, params Expression<Func<T, TValue>>[] groupPropertySelectors)
		{
			GroupBy(aggregationOperation, groupPropertySelectors.Select(GetMemberQueryPath).ToArray());
			return this;
		}

		/// <summary>
		/// Partition the query so we can intersect different parts of the query
		/// across different index entries.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Intersect()
		{
			Intersect();
			return this;
		}

		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Statistics(out RavenQueryStatistics stats)
		{
			Statistics(out stats);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.UsingDefaultField(string field)
		{
			UsingDefaultField(field);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
		{
			UsingDefaultOperator(queryOperator);
			return this;
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(string path)
		{
			Include(path);
			return this;
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
		{
			Include(path);
			return this;
		}

		/// <summary>
		/// Negate the next operation
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Not
		{
			get
			{
				NegateNext();
				return this;
			}
		}

		/// <summary>
		/// Takes the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Take(int count)
		{
			Take(count);
			return this;
		}

		/// <summary>
		/// Skips the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Skip(int count)
		{
			Skip(count);
			return this;
		}

		/// <summary>
		/// Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Where(string whereClause)
		{
			Where(whereClause);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to NotAnalyzed
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, object value)
		{
			WhereEquals(fieldName, value);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to NotAnalyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereEquals(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to allow wildcards only if analyzed
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool isAnalyzed)
		{
			WhereEquals(fieldName, value, isAnalyzed);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to allow wildcards only if analyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool isAnalyzed)
		{
			WhereEquals(GetMemberQueryPath(propertySelector.Body), value, isAnalyzed);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(WhereParams whereEqualsParams)
		{
			WhereEquals(whereEqualsParams);
			return this;
		}

		/// <summary>
		/// Check that the field has one of the specified value
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values)
		{
			WhereIn(fieldName, values);
			return this;
		}

		/// <summary>
		/// Check that the field has one of the specified value
		/// </summary>
		public IDocumentQuery<T> WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
		{
			WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
			return this;
		}

		/// <summary>
		/// Matches fields which starts with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereStartsWith(string fieldName, object value)
		{
			WhereStartsWith(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields which starts with the specified value.
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Matches fields which ends with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEndsWith(string fieldName, object value)
		{
			WhereEndsWith(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields which ends with the specified value.
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end)
		{
			WhereBetween(fieldName, start, end);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		public IDocumentQuery<T> WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
		{
			WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetweenOrEqual(string fieldName, object start, object end)
		{
			WhereBetweenOrEqual(fieldName, start, end);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		public IDocumentQuery<T> WhereBetweenOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
		{
			WhereBetweenOrEqual(GetMemberQueryPath(propertySelector.Body), start, end);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value)
		{
			WhereGreaterThan(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value)
		{
			WhereGreaterThanOrEqual(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThan(string fieldName, object value)
		{
			WhereLessThan(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereLessThan(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value)
		{
			WhereLessThanOrEqual(fieldName, value);
			return this;
		}

		/// <summary>
		///   Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name = "propertySelector">Property selector for the field.</param>
		/// <param name = "value">The value.</param>
		public IDocumentQuery<T> WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
		{
			WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
			return this;
		}

		/// <summary>
		/// Add an AND to the query
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AndAlso()
		{
			AndAlso();
			return this;
		}

		/// <summary>
		/// Add an OR to the query
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrElse()
		{
			OrElse();
			return this;
		}

		/// <summary>
		/// Specifies a boost weight to the last where clause.
		/// The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Boost(decimal boost)
		{
			Boost(boost);
			return this;
		}

		/// <summary>
		/// Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Fuzzy(decimal fuzzy)
		{
			Fuzzy(fuzzy);
			return this;
		}

		/// <summary>
		/// Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name="proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Proximity(int proximity)
		{
			Proximity(proximity);
			return this;
		}

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		public IDocumentQuery<T> WithinRadiusOf(double radius, double latitude, double longitude)
		{
			return (IDocumentQuery<T>) GenerateQueryWithinRadiusOf(Constants.DefaultSpatialFieldName, radius, latitude, longitude);
		}

		public IDocumentQuery<T> WithinRadiusOf(string fieldName, double radius, double latitude, double longitude)
		{
			return (IDocumentQuery<T>)GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude);
		}

		public IDocumentQuery<T> RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel, double distanceErrorPct = 0.025)
		{
			return (IDocumentQuery<T>)GenerateSpatialQueryData(fieldName, shapeWKT, rel, distanceErrorPct);
		}

		/// <summary>
		///   Filter matches to be inside the specified radius
		/// </summary>
		/// <param name = "radius">The radius.</param>
		/// <param name = "latitude">The latitude.</param>
		/// <param name = "longitude">The longitude.</param>
		protected override object GenerateQueryWithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distanceErrorPct = 0.025)
		{
			return GenerateSpatialQueryData(fieldName, SpatialIndexQuery.GetQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.Within, distanceErrorPct);
		}

		protected override object GenerateSpatialQueryData(string fieldName, string shapeWKT, SpatialRelation relation, double distanceErrorPct)
		{
			isSpatialQuery = true;
			spatialFieldName = fieldName;
			queryShape = shapeWKT;
			spatialRelation = relation;
			this.distanceErrorPct = distanceErrorPct;
			return this;
		}

		/// <summary>
		/// Sorts the query results by distance.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.SortByDistance()
		{
			OrderBy(Constants.DistanceFieldName);
			return this;
		}

		/// <summary>
		/// Order the results by the specified fields
		/// The fields are the names of the fields to sort, defaulting to sorting by ascending.
		/// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy(params string[] fields)
		{
			OrderBy(fields);
			return this;
		}

		/// <summary>
		///   Order the results by the specified fields
		///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
		///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name = "propertySelectors">Property selectors for the fields.</param>
		public IDocumentQuery<T> OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
		{
			OrderBy(propertySelectors.Select(GetMemberQueryPath).ToArray());
			return this;
		}

		/// <summary>
		/// Order the results by the specified fields
		/// The fields are the names of the fields to sort, defaulting to sorting by descending.
		/// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDescending(params string[] fields)
		{
			OrderByDescending(fields);
			return this;
		}

		/// <summary>
		///   Order the results by the specified fields
		///   The fields are the names of the fields to sort, defaulting to sorting by descending.
		///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name = "propertySelectors">Property selectors for the fields.</param>
		public IDocumentQuery<T> OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
		{
			OrderByDescending(propertySelectors.Select(GetMemberQueryPath).ToArray());
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Highlight(
			string fieldName, 
			int fragmentLength, 
			int fragmentCount, 
			string fragmentsField)
		{
			Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Highlight(
			string fieldName, 
			int fragmentLength, 
			int fragmentCount, 
			out FieldHighlightings highlightings)
		{
			this.Highlight(fieldName, fragmentLength, fragmentCount, out highlightings);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Highlight<TValue>(
			Expression<Func<T, TValue>> propertySelector, 
			int fragmentLength, 
			int fragmentCount,
			Expression<Func<T, IEnumerable>> fragmentsPropertySelector)
		{
			var fieldName = this.GetMemberQueryPath(propertySelector);
			var fragmentsField = this.GetMemberQueryPath(fragmentsPropertySelector);
			this.Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Highlight<TValue>(
			Expression<Func<T, TValue>> propertySelector, 
			int fragmentLength, 
			int fragmentCount,
			out FieldHighlightings fieldHighlightings)
		{
			this.Highlight(this.GetMemberQueryPath(propertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.SetHighlighterTags(string preTag, string postTag)
		{
			this.SetHighlighterTags(new[]{preTag},new[]{postTag});
			return this;
		}

		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.SetHighlighterTags(string[] preTags, string[] postTags)
		{
			this.SetHighlighterTags(preTags, postTags);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow()
		{
			WaitForNonStaleResultsAsOfNow();
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite()
		{
			WaitForNonStaleResultsAsOfLastWrite();
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfLastWrite(waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfNow(waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff)
		{
			WaitForNonStaleResultsAsOf(cutOff);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff etag.
		/// </summary>
		/// <param name="cutOffEtag">The cut off etag.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(Guid cutOffEtag)
		{
			WaitForNonStaleResultsAsOf(cutOffEtag);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
		/// </summary>
		/// <param name="cutOffEtag">The cut off etag.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(Guid cutOffEtag, TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
			return this;
		}

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResults()
		{
			WaitForNonStaleResults();
			return this;
		}


		/// <summary>
		/// Allows you to modify the index query before it is sent to the server
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.BeforeQueryExecution(Action<IndexQuery> beforeQueryExecution)
		{
			BeforeQueryExecution(beforeQueryExecution);
			return this;
		}

		/// <summary>
		/// Returns an enumerator that iterates through a collection.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		/// <summary>
		///   Returns a <see cref = "System.String" /> that represents this instance.
		/// </summary>
		/// <returns>
		///   A <see cref = "System.String" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var query = base.ToString();
			if (isSpatialQuery)
				return string.Format(CultureInfo.InvariantCulture, "{0} SpatialField: {1} QueryShape: {2} Relation: {3}", query, spatialFieldName, queryShape, spatialRelation);
			return query;
		}
	}
}
#endif
