// -----------------------------------------------------------------------
//  <copyright file="From46To47.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
	public class From47To48 : ISchemaUpdate
	{
	    private InMemoryRavenConfiguration configuration;
	    public string FromSchemaVersion { get { return "4.7"; } }

		public void Init(IUuidGenerator generator, InMemoryRavenConfiguration configuration)
		{
		    this.configuration = configuration;
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
                Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
                using (var sr = new Table(session, dbid, table, OpenTableGrbit.None)) 
                {
                    Api.JetRenameColumn(session, sr, "view", "view_old", RenameColumnGrbit.None);
                    JET_COLUMNID columnid;
                    Api.JetAddColumn(session, sr, "view", new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.Long,
                        grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);
                }
                Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
            }

            // So first we allocate ids and crap
            // and write that to disk in a safe manner
            // I might want to look at keeping a list of written files to delete if it all goes tits up at any point
		    var filesToDelete = new List<string>();
		    var nameToIds = new Dictionary<string, int>();
		    var indexDefPath = Path.Combine(configuration.DataDirectory, "IndexDefinitions");

            var indexDefinitions = Directory.GetFiles(indexDefPath, "*.index")
                                        .Select(x => { filesToDelete.Add(x); return x; })
                                        .Select(index => JsonConvert.DeserializeObject<IndexDefinition>(File.ReadAllText(index), Default.Converters))
                                        .ToArray();
            var transformDefinitions = Directory.GetFiles(indexDefPath, "*.transform")
                                        .Select(x => { filesToDelete.Add(x); return x; })
                                        .Select(index => JsonConvert.DeserializeObject<TransformerDefinition>(File.ReadAllText(index), Default.Converters))
                                        .ToArray();

		    for (var i = 0; i < indexDefinitions.Length; i++)
		    {
		        var definition = indexDefinitions[i];
		        definition.IndexId = i;
		        nameToIds[definition.Name] = definition.IndexId;
		        var path = Path.Combine(indexDefPath, definition.IndexId + ".index");

                // TODO: This can fail, rollback
                File.WriteAllText(path, JsonConvert.SerializeObject(definition, Formatting.Indented, Default.Converters));

                var indexDirectory = IndexDefinitionStorage.FixupIndexName(definition.Name, configuration.IndexStoragePath);
		        var oldStorageDirectory = Path.Combine(configuration.IndexStoragePath, MonoHttpUtility.UrlEncode(indexDirectory));
                var newStorageDirectory = Path.Combine(configuration.IndexStoragePath, definition.IndexId.ToString());

                // TODO: This can fail, rollback
                Directory.Move(oldStorageDirectory, newStorageDirectory);
		    }

		    for (var i = 0; i < transformDefinitions.Length; i++)
		    {
		        var definition = transformDefinitions[i];
		        definition.IndexId = indexDefinitions.Length + i;
		        nameToIds[definition.Name] = definition.IndexId;
		        var path = Path.Combine(indexDefPath, definition.IndexId + ".transform");

                // TODO: This can file, rollback
                File.WriteAllText(path, JsonConvert.SerializeObject(definition, Formatting.Indented, Default.Converters));
		    }


            // Now we need to go through all the tables and do a look-up of 'view' to 'id' and write that data
		    foreach (var tableName in tables)
		    {
		        using (var table = new Table(session, dbid, tableName, OpenTableGrbit.None))
		        {
		            var rows = 0;
		            Api.MoveBeforeFirst(session, table);
		            while (Api.TryMoveNext(session, table))
		            {
		                var viewNameId = Api.GetTableColumnid(session, table, "view_old");
		                var viewIdId = Api.GetTableColumnid(session, table, "view");

		                using (var update = new Update(session, table, JET_prep.Replace))
		                {
		                    var viewName = Api.RetrieveColumnAsString(session, table, viewNameId, Encoding.Unicode);
		                    Api.SetColumn(session, table, viewIdId, nameToIds[viewName]); 
		                    update.Save();
		                }

		                if (rows++%10000 == 0)
		                {
		                    output("Processed " + (rows - 1) + " rows in " + tableName);
		                    continue;
		                }
		            }

                    Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
                    Api.JetDeleteColumn(session, table, "view_old");
                    Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
		        }
		    }

            filesToDelete.ForEach(File.Delete);
			SchemaCreator.UpdateVersion(session, dbid, "4.8");
		} 
	}
}