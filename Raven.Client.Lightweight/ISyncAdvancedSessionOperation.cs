//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;

using Raven.Client.Document.Batches;
using Raven.Client.Indexes;

namespace Raven.Client
{
	/// <summary>
	/// Advanced synchronous session operations
	/// </summary>
	public interface ISyncAdvancedSessionOperation : IAdvancedDocumentSessionOperations
	{
		/// <summary>
		/// Refreshes the specified entity from Raven server.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Refresh<T>(T entity);

		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25);

		/// <summary>
		/// Access the lazy operations
		/// </summary>
		ILazySessionOperations Lazily { get; }

		/// <summary>
		/// Access the eager operations
		/// </summary>
		IEagerSessionOperations Eagerly { get; }

		/// <summary>
		/// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
		IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false);

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		IDocumentQuery<T> LuceneQuery<T>();

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		string GetDocumentUrl(object entity);
	}
}
#endif