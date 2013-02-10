//-----------------------------------------------------------------------
// <copyright file="CascadeDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.CascadeDelete
{
	public class CascadeDeleteTrigger : AbstractDeleteTrigger
	{
		public override void OnDelete(string key, TransactionInformation transactionInformation)
		{
			if (CascadeDeleteContext.IsInCascadeDeleteContext)
				return;

			using(Database.DisableAllTriggersForCurrentThread())
			using (CascadeDeleteContext.Enter())
			{
				var document = Database.Get(key, transactionInformation);
				if (document == null)
					return;


				var documentsToDelete = document.Metadata.Value<RavenJArray>(MetadataKeys.DocumentsToCascadeDelete);

				if (documentsToDelete != null)
				{
					foreach (var documentToDelete in documentsToDelete)
					{
						var documentId = documentToDelete.Value<string>();
						if (!CascadeDeleteContext.HasAlreadyDeletedDocument(documentId))
						{
							CascadeDeleteContext.AddDeletedDocument(documentId);
							Database.Delete(documentId, null, transactionInformation);
						}
					}
				}

				var attachmentsToDelete = document.Metadata.Value<RavenJArray>(MetadataKeys.AttachmentsToCascadeDelete);

				if (attachmentsToDelete != null)
					foreach (var attachmentToDelete in attachmentsToDelete)
						Database.DeleteStatic(attachmentToDelete.Value<string>(), null);
			}
		}
	}
}
