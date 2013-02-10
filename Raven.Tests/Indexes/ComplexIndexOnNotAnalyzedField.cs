//-----------------------------------------------------------------------
// <copyright file="ComplexIndexOnNotAnalyzedField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class ComplexIndexOnNotAnalyzedField: RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public ComplexIndexOnNotAnalyzedField()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanQueryOnKey()
		{
			db.Put("companies/", null,
			       RavenJObject.Parse("{'Name':'Hibernating Rhinos', 'Partners': ['companies/49', 'companies/50']}"), 
				   RavenJObject.Parse("{'Raven-Entity-Name': 'Companies'}"),
			       null);


			db.PutIndex("CompaniesByPartners", new IndexDefinition
			{
				Map = "from company in docs.Companies from partner in company.Partners select new { Partner = partner }",
			});

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("CompaniesByPartners", new IndexQuery
				{
					Query = "Partner:companies/49",
					PageSize = 10
				});
			} while (queryResult.IsStale);

			Assert.Equal("Hibernating Rhinos", queryResult.Results[0].Value<string>("Name"));
		}
	}
}
