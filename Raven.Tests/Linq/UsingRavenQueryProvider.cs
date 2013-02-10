//-----------------------------------------------------------------------
// <copyright file="UsingRavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Xunit;
using Raven.Database.Data;
using Raven.Client;
using System.IO;
using Raven.Client.Document;
using Raven.Client.Linq;
using System.Threading;
using System.Diagnostics;

namespace Raven.Tests.Linq
{
	public class UsingRavenQueryProvider : RavenTest
	{
		[Fact]
		public void Can_perform_Skip_Take_Query()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					AddData(session);

					db.DatabaseCommands.DeleteIndex(indexName);
					db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs
											  select new { doc.Name, doc.Age },
								SortOptions = { { x => x.Name, SortOptions.StringVal } }
							}, true);

					WaitForQueryToComplete(session, indexName);

					var allResults = session.Query<User>(indexName).OrderBy(x => x.Name)
											.Where(x => x.Age > 0);
					Assert.Equal(4, allResults.ToArray().Count());

					var takeResults = session.Query<User>(indexName).OrderBy(x => x.Name)
											.Where(x => x.Age > 0)
											.Take(3);
					//There are 4 items of data in the db, but using Take(1) means we should only see 4
					Assert.Equal(3, takeResults.ToArray().Count());

					var skipResults = session.Query<User>(indexName).OrderBy(x => x.Name)
											.Where(x => x.Age > 0)
											.Skip(1);
					//Using Skip(1) means we should only see the last 3
					Assert.Equal(3, skipResults.ToArray().Count());
					Assert.DoesNotContain(firstUser, skipResults.ToArray());

					var skipTakeResults = session.Query<User>(indexName).OrderBy(x => x.Name)
											.Where(x => x.Age > 0)
											.Skip(1)
											.Take(2);
					//Using Skip(1), Take(2) means we shouldn't see the 1st or 4th (last) users
					Assert.Equal(2, skipTakeResults.ToArray().Count());
					Assert.DoesNotContain<User>(firstUser, skipTakeResults.ToArray());
					Assert.DoesNotContain<User>(lastUser, skipTakeResults.ToArray());
				}
			}
		}

		[Fact]
		public void Can_perform_First_and_FirstOrDefault_Query()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					AddData(session);

					db.DatabaseCommands.DeleteIndex(indexName);
					var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs
											  select new { doc.Name, doc.Age },
								SortOptions = { { x => x.Name, SortOptions.StringVal } }
							}, true);

					WaitForQueryToComplete(session, indexName);

					var firstItem = session.Query<User>(indexName).OrderBy(x => x.Name)
											.First();
					Assert.Equal(firstUser, firstItem);

					//This should pull out the 1st parson ages 60, i.e. "Bob"
					var firstAgeItem = session.Query<User>(indexName)
											.First(x => x.Age == 60);
					Assert.Equal("Bob", firstAgeItem.Name);

					//No-one is aged 15, so we should get null
					var firstDefaultItem = session.Query<User>(indexName)
											.FirstOrDefault(x => x.Age == 15);
					Assert.Null(firstDefaultItem);
				}
			}
		}

		[Fact]
		public void Can_perform_Single_and_SingleOrDefault_Query()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					AddData(session);

					db.DatabaseCommands.DeleteIndex(indexName);
					var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs
											  select new { doc.Name, doc.Age },
								Indexes = { { x => x.Name, FieldIndexing.Analyzed } }
							}, true);

					WaitForQueryToComplete(session, indexName);

					var singleItem = session.Query<User>(indexName)
											.Single(x => x.Name == ("James"));
					Assert.Equal(25, singleItem.Age);
					Assert.Equal("James", singleItem.Name);

					//A default query should return for results, so Single() should throw
					Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).Single());
					//A query of age = 30 should return for 2 results, so Single() should throw
					Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).Single(x => x.Age == 30));

					//A query of age = 30 should return for 2 results, so SingleOrDefault() should also throw
					Assert.Throws(typeof(InvalidOperationException), () => session.Query<User>(indexName).SingleOrDefault(x => x.Age == 30));

					//A query of age = 75 should return for NO results, so SingleOrDefault() should return a default value
					var singleOrDefaultItem = session.Query<User>(indexName)
											.SingleOrDefault(x => x.Age == 75);
					Assert.Null(singleOrDefaultItem);
				}
			}
		}

		[Fact]
		public void Can_perform_Boolean_Queries()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					session.Store(new User() { Name = "Matt", Info = "Male Age 25" }); //Active = false by default
					session.Store(new User() { Name = "Matt", Info = "Male Age 28", Active = true });
					session.Store(new User() { Name = "Matt", Info = "Male Age 35", Active = false });
					session.SaveChanges();

					db.DatabaseCommands.DeleteIndex(indexName);
					var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs
											  select new { doc.Name, doc.Age, doc.Info, doc.Active },
								Indexes = { { x => x.Name, FieldIndexing.Analyzed } }
							}, true);

					WaitForQueryToComplete(session, indexName);

					var testQuery = session.Query<User>(indexName)
										.Where(x => x.Name == ("Matt") && x.Active);
					Assert.Equal(1, testQuery.ToArray().Count());
					foreach (var testResult in testQuery)
						Assert.True(testResult.Active);

					testQuery = session.Query<User>(indexName)
										.Where(x => x.Name == ("Matt") && !x.Active);
					Assert.Equal(2, testQuery.ToArray().Count());
					foreach (var testResult in testQuery)
						Assert.False(testResult.Active);
				}
			}
		}

		[Fact]
		public void Can_perform_DateTime_Comparison_Queries()
		{

			DateTime firstTime = SystemTime.UtcNow;
			DateTime secondTime = firstTime.AddMonths(1);  // use .AddHours(1) to get a second bug, timezone related
			DateTime thirdTime = secondTime.AddMonths(1);  // use .AddHours(1) to get a second bug, timezone related

			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					session.Store(new User { Name = "First", Created = firstTime });
					session.Store(new User { Name = "Second", Created = secondTime });
					session.Store(new User { Name = "Third", Created = thirdTime });
					session.SaveChanges();

					db.DatabaseCommands.DeleteIndex(indexName);
					var result = db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs
											  select new { doc.Name, doc.Created },
							}, true);

					WaitForQueryToComplete(session, indexName);

					Assert.Equal(3, session.Query<User>(indexName).ToArray().Length);

					var testQuery = session.Query<User>(indexName)
										.Where(x => x.Created > secondTime)
										.ToArray();
					Assert.Equal(1, testQuery.Count());
					Assert.True(testQuery.Select(q => q.Name).Contains("Third"));

					testQuery = session.Query<User>(indexName)
										.Where(x => x.Created >= secondTime)
										.ToArray();
					Assert.Equal(2, testQuery.Count());
					Assert.True(testQuery.Select(q => q.Name).Contains("Third"));
					Assert.True(testQuery.Select(q => q.Name).Contains("Second"));

					testQuery = session.Query<User>(indexName)
										.Where(x => x.Created < secondTime)
										.ToArray();
					Assert.Equal(1, testQuery.Count());
					Assert.True(testQuery.Select(q => q.Name).Contains("First"));

					testQuery = session.Query<User>(indexName)
										.Where(x => x.Created <= secondTime)
										.ToArray();
					Assert.Equal(2, testQuery.Count());
					Assert.True(testQuery.Select(q => q.Name).Contains("First"));
					Assert.True(testQuery.Select(q => q.Name).Contains("Second"));

					testQuery = session.Query<User>(indexName)
										.Where(x => x.Created == secondTime)
										.ToArray();
					Assert.Equal(1, testQuery.Count());
					Assert.True(testQuery.Select(q => q.Name).Contains("Second"));
				}
			}
		}

		[Fact] // See issue #105 (http://github.com/ravendb/ravendb/issues/#issue/105)
		public void Does_Not_Ignore_Expressions_Before_Where()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				string indexName = "UserIndex";
				using (var session = db.OpenSession())
				{
					session.Store(new User() { Name = "Third", Age = 18 });
					session.Store(new User() { Name = "First", Age = 10 });
					session.Store(new User() { Name = "Second", Age = 20 });
					session.SaveChanges();

					db.DatabaseCommands.DeleteIndex(indexName);
					db.DatabaseCommands.PutIndex<User, User>(indexName,
							new IndexDefinitionBuilder<User, User>()
							{
								Map = docs => from doc in docs select new { doc.Name, doc.Age },
							}, true);

					WaitForQueryToComplete(session, indexName);

					var result = session.Query<User>(indexName).OrderBy(x => x.Name).Where(x => x.Age >= 18).ToList();

					Assert.Equal(2, result.Count());

					Assert.Equal("Second", result[0].Name);
					Assert.Equal("Third", result[1].Name);
				}
			}
		}

		[Fact] // See issue #145 (http://github.com/ravendb/ravendb/issues/#issue/145)
		public void Can_Use_Static_Fields_In_Where_Clauses()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				db.DatabaseCommands.PutIndex("DateTime",
						new IndexDefinition
						{
							Map = @"from info in docs.DateTimeInfos                                    
									select new { info.TimeOfDay }",
						});

				var currentTime = SystemTime.UtcNow;
				using (var s = db.OpenSession())
				{
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(1) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(2) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromMinutes(1) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromSeconds(10) });

					s.SaveChanges();
				}

				using (var s = db.OpenSession())
				{
					//Just issue a blank query to make sure there are no stale results                    
					var test = s.Query<DateTimeInfo>("DateTime")
								.Customize(x => x.WaitForNonStaleResults())
								.Where(x => x.TimeOfDay > currentTime)
								.ToArray();

					IQueryable<DateTimeInfo> testFail = null;
					Assert.DoesNotThrow(() =>
						{
							testFail = s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay > DateTime.MinValue); // =====> Throws an exception
						});
					Assert.NotEqual(null, testFail);

					var dt = DateTime.MinValue;
					var testPass = s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay > dt); //=====>Works

					Assert.Equal(testPass.Count(), testFail.Count());
				}
			}
		}

		public void Can_Use_Static_Properties_In_Where_Clauses()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				db.DatabaseCommands.PutIndex("DateTime",
						new IndexDefinition
						{
							Map = @"from info in docs.DateTimeInfos                                    
									select new { info.TimeOfDay }",
						});

				using (var s = db.OpenSession())
				{
					s.Store(new DateTimeInfo { TimeOfDay = SystemTime.UtcNow.AddDays(1) });
					s.Store(new DateTimeInfo { TimeOfDay = SystemTime.UtcNow.AddDays(-1) });
					s.Store(new DateTimeInfo { TimeOfDay = SystemTime.UtcNow.AddDays(1) });
					s.SaveChanges();
				}

				using (var s = db.OpenSession())
				{
					//Just issue a blank query to make sure there are no stale results                    
					s.Query<DateTimeInfo>("DateTime")
						.Customize(x => x.WaitForNonStaleResults()).FirstOrDefault();

					var count = s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay > SystemTime.UtcNow).Count();
					Assert.Equal(2, count);
				}
			}
		}

		[Fact] // See issue #145 (http://github.com/ravendb/ravendb/issues/#issue/145)
		public void Can_use_inequality_to_compare_dates()
		{
			using (var db = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				db.Initialize();

				db.DatabaseCommands.PutIndex("DateTime",
						new IndexDefinition
						{
							Map = @"from info in docs.DateTimeInfos                                    
									select new { info.TimeOfDay }",
						});

				var currentTime = SystemTime.UtcNow;
				using (var s = db.OpenSession())
				{
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(1) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromHours(2) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromMinutes(1) });
					s.Store(new DateTimeInfo { TimeOfDay = currentTime + TimeSpan.FromSeconds(10) });

					s.SaveChanges();
				}

				using (var s = db.OpenSession())
				{
					//Just issue a blank query to make sure there are no stale results                    
					var test = s.Query<DateTimeInfo>("DateTime")
								.Customize(x => x.WaitForNonStaleResults())
								.Where(x => x.TimeOfDay > currentTime)
								.ToArray();


					Assert.NotEmpty(s.Query<DateTimeInfo>("DateTime").Where(x => x.TimeOfDay != DateTime.MinValue));
				}
			}
		}

		[Fact] // See issue #91 http://github.com/ravendb/ravendb/issues/issue/91 and 
		//discussion here http://groups.google.com/group/ravendb/browse_thread/thread/3df57d19d41fc21
		public void Can_do_projection_in_query_result()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				store.DatabaseCommands.PutIndex("ByLineCost",
						new IndexDefinition
						{
							Map = @"from order in docs.Orders
									from line in order.Lines
									select new { Cost = line.Cost }",

							Stores = { { "Cost", FieldStorage.Yes } }
						});

				using (var s = store.OpenSession())
				{
					s.Store(new Order
					{
						Lines = new List<OrderItem>
						{
							new OrderItem { Cost = 1.59m, Quantity = 5 },
							new OrderItem { Cost = 7.59m, Quantity = 3 }
						},
					});
					s.Store(new Order
					{
						Lines = new List<OrderItem>
						{
							new OrderItem { Cost = 0.59m, Quantity = 9 },                            
						},
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					//Just issue a blank query to make sure there are no stale results    
					WaitForQueryToComplete(s, "ByLineCost");

					//This is the lucene query we want to mimic
					var luceneResult = s.Advanced.LuceneQuery<OrderItem>("ByLineCost")
							.Where("Cost_Range:{Dx1 TO NULL}")
							.SelectFields<SomeDataProjection>("Cost")
							.ToArray();

					var projectionResult = s.Query<OrderItem>("ByLineCost")
						.Where(x => x.Cost > 1)
						.Select(x => new SomeDataProjection { Cost = x.Cost })
						.ToArray();

					Assert.Equal(luceneResult.Count(), projectionResult.Count());
					int counter = 0;
					foreach (var item in luceneResult)
					{
						Assert.Equal(item.Cost, projectionResult[counter].Cost);
						counter++;
					}
				}
			}
		}

		public class SomeDataProjection
		{
			public decimal Cost { get; set; }
		}

		private class Order
		{
			public string Id { get; set; }
			//public decimal Test { get; set; }
			public List<OrderItem> Lines { get; set; }
		}

		private enum Origin
		{
			Africa, UnitedStates
		}

		private class OrderItem
		{
			public Guid Id { get; set; }
			public Guid CustomerId { get; set; }
			public decimal Cost { get; set; }
			public decimal Quantity { get; set; }
			public Origin Country { get; set; }
			public string Description { get; set; }
		}

		private class DateTimeInfo
		{
			public string Id { get; set; }
			public DateTime TimeOfDay { get; set; }
		}

		private static void WaitForQueryToComplete(IDocumentSession session, string indexName)
		{
			QueryResult results;
			do
			{
				//doesn't matter what the query is here, just want to see if it's stale or not
				results = session.Advanced.LuceneQuery<User>(indexName)
							  .Where("")
							  .QueryResult;

				if (results.IsStale)
					Thread.Sleep(1000);
			} while (results.IsStale);
		}

		private readonly User firstUser = new User { Name = "Alan", Age = 30 };
		private readonly User lastUser = new User { Name = "Zoe", Age = 30 };

		private void AddData(IDocumentSession documentSession)
		{
			documentSession.Store(firstUser);
			documentSession.Store(new User { Name = "James", Age = 25 });
			documentSession.Store(new User { Name = "Bob", Age = 60 });
			documentSession.Store(lastUser);

			documentSession.SaveChanges();
		}

		[Fact]
		public void Can_Use_In_Array_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5 });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3 });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4 });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3 });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Quantity.In(new[] { 3m, 5m })
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 3);
				}
			}
		}

		[Fact]
		public void Can_Use_Strings_In_Array_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5, Description = "First" });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3, Description = "Second" });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4, Description = "Third" });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3, Description = "Fourth" });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var ravenQueryable = (from item in s.Query<OrderItem>()
						                      .Customize(x => x.WaitForNonStaleResults())
					                      where item.Description.In(new[] {"", "First"})
					                      select item
					                     );
					var items = ravenQueryable.ToArray();


					Assert.Equal(items.Length, 1);

				}

				using (var s2 = store.OpenSession())
				{
					var ravenQueryable2 = (from item in s2.Query<OrderItem>()
										  .Customize(x => x.WaitForNonStaleResults())
										  where item.Description.In(new[] { "First", "" })
										  select item
										 );
					var items2 = ravenQueryable2.ToArray();


					Assert.Equal(items2.Length, 1);
				}
			}
		}


		[Fact]
		public void Can_Use_Enums_In_Array_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5, Country = Origin.Africa });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3, Country = Origin.Africa });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4, Country = Origin.UnitedStates });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3, Country = Origin.Africa });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Country.In(new[] { Origin.UnitedStates })
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 1);
				}
			}
		}
		[Fact]
		public void Can_Use_Enums_In_IEnumerable_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5, Country = Origin.Africa });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3, Country = Origin.Africa });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4, Country = Origin.UnitedStates });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3, Country = Origin.Africa });
					s.SaveChanges();
				}

				var list = new List<Origin> { Origin.Africa };

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Country.In(list)
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 3);
				}
			}
		}

		[Fact]
		public void Can_Use_In_IEnumerable_In_Where_Clause_with_negation()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5 });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3 });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4 });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3 });
					s.SaveChanges();
				}

				var list = new List<decimal> { 3, 5 };

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where !item.Quantity.In(list)
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 1);
				}
			}
		}

		[Fact]
		public void Can_Use_In_Params_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5 });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3 });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4 });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3 });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Quantity.In(3m, 5m)
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 3);
				}
			}
		}
		[Fact]
		public void Can_Use_In_IEnumerable_In_Where_Clause()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();

				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 5 });
					s.Store(new OrderItem { Cost = 7.59m, Quantity = 3 });
					s.Store(new OrderItem { Cost = 1.59m, Quantity = 4 });
					s.Store(new OrderItem { Cost = 1.39m, Quantity = 3 });
					s.SaveChanges();
				}

				var list = new List<decimal> { 3, 5 };

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Quantity.In(list)
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 3);
				}
			}
		}
		[Fact]
		public void Can_Use_In_IEnumerable_Not_In_Where_Clause_on_Id()
		{
			using (var store = new EmbeddableDocumentStore() { RunInMemory = true })
			{
				store.Initialize();
				var guid1 = Guid.NewGuid();
				var guid2 = Guid.NewGuid();
				var customerId = Guid.NewGuid();
				using (var s = store.OpenSession())
				{
					s.Store(new OrderItem { Id = Guid.NewGuid(), CustomerId = customerId, Cost = 1.59m, Quantity = 5 });
					s.Store(new OrderItem { Id = guid1, CustomerId = customerId, Cost = 7.59m, Quantity = 3 });
					s.Store(new OrderItem { Id = guid2, CustomerId = customerId, Cost = 1.59m, Quantity = 4 });
					s.Store(new OrderItem { Id = Guid.NewGuid(), CustomerId = customerId, Cost = 1.39m, Quantity = 3 });
					s.SaveChanges();
				}

				var list = new List<Guid> { guid1 }; //, guid2 };

				using (var s = store.OpenSession())
				{
					var items = (from item in s.Query<OrderItem>()
								 where item.Quantity > 4 && item.CustomerId == customerId
														&& !item.Id.In(list)
								 select item
									 ).ToArray();

					Assert.Equal(items.Length, 1);
				}
			}
		}

	}
}
