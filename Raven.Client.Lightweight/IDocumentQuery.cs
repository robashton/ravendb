//-----------------------------------------------------------------------
// <copyright file="IDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	public interface IDocumentQuery<T> : IEnumerable<T>, IDocumentQueryBase<T, IDocumentQuery<T>>
	{

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections);

		/// <summary>
		/// Selects the projection fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		IDocumentQuery<TProjection> SelectFields<TProjection>();


#if !SILVERLIGHT
		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		QueryResult QueryResult { get; }
#endif

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		Lazy<IEnumerable<T>> Lazily();

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed.
		/// Also provide a function to execute when the value is evaluated
		/// </summary>
		Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval);

		/// <summary>
		/// Create the index query object for this query
		/// </summary>
		IndexQuery GetIndexQuery(bool isAsync);
	}
}