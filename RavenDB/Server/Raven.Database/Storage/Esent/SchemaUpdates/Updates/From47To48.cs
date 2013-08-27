// -----------------------------------------------------------------------
//  <copyright file="From46To47.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From47To48 : ISchemaUpdate
	{
		public string FromSchemaVersion { get { return "4.7"; } }

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
            //SCheduledReductionsTable
            // "scheduled_reductions",
            //    view
            //MapResultsTable
            //mapped_results
            //    view
            //ReduceResultsTable
            //reduce_results
            //    view
            //ReduceKeysCountsTable
            // reduce_keys_counts
            //    view
            //ReduceKeysStatusTable
            // reduce_keys_status
            //    view
            //IndexedDocumentsReferencesTable
            // indexed_documents_references
            //    view

		    var tables = new[] { "scheduled_reductions", "mapped_results", "reduce_results", "reduce_keys_counts", "reduce_keys_status", "indexed_documents_references"};
            foreach(var table in tables)
		    {
		        using (var sr = new Table(session, dbid, table, OpenTableGrbit.None)) 
                {
                    //Api.JetRenameColumn(session, sr, "view", "view_old", RenameColumnGrbit.None);
                    JET_COLUMNID columnid;
                    Api.JetAddColumn(session, sr, "view_new", new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.Long,
                        grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);
                }
		    }

            // Load all the indexes so we have the names
            // QUESTION: How best to do this?
            // Create a dictionary of integers/names

            // Update all the view columns with those integers
            // QUESTION: How do I iterate through the whole table and do this?

            // Rename the disk artifacts
           

			SchemaCreator.UpdateVersion(session, dbid, "4.8");
		} 
	}
}