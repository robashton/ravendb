//-----------------------------------------------------------------------
// <copyright file="IDocumentStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IDocumentStorageActions 
	{
		IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take);
		IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag, int take, long? maxSize = null, Guid? untilEtag = null);
		IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take);

		long GetDocumentsCount();

		JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation);
		JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation);

		bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata, out Guid? deletedETag);
		AddDocumentResult AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata);
		AddDocumentResult PutDocumentMetadata(string key, RavenJObject metadata);

		void IncrementDocumentCount(int value);
		AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool checkForUpdates);

		void TouchDocument(string key, out Guid? preTouchEtag, out Guid? afterTouchEtag);
		Guid GetBestNextDocumentEtag(Guid etag);
	}

	public class AddDocumentResult
	{
		public Guid Etag;
		public DateTime SavedAt;
		public bool Updated;
	}
}
