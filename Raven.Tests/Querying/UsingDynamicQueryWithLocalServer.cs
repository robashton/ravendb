//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithLocalServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Querying
{
	public class UsingDynamicQueryWithLocalServer : RavenTest
	{
		[Fact]
		public void CanPerformDynamicQueryUsingClientLinqQueryWithNestedCollection()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens",
				 Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Birds" }
				 }
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos",
				Tags = new BlogTag[]{
					 new BlogTag(){ Name = "Mammals" }
				 }
			};

			using(var store = this.NewDocumentStore())
			{               
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5)))
						.Where(x => x.Tags.Any(y=>y.Name == "Birds"))
						.ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("one", results[0].Title);
					Assert.Equal("Ravens", results[0].Category);
				}
			}
		}

		[Fact]
		public void CanPerformDynamicQueryUsingClientLinqQuery()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			using(var store = this.NewDocumentStore())
			{               
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
						.ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("two", results[0].Title);
					Assert.Equal("Rhinos", results[0].Category);
				}
			}
		}

		[Fact]
		public void QueryForASpecificTypeDoesNotBringBackOtherTypes()
		{
			using (var store = this.NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new BlogTag());
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>()
						.Select(b=> new { b.Category})
						.ToArray();
					Assert.Equal(0, results.Length);
				}
			}
		}

		[Fact]
		public void CanPerformDynamicQueryUsingClientLuceneQuery()
		{
			var blogOne = new Blog
			{
				Title = "one",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "two",
				Category = "Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "three",
				Category = "Rhinos"
			};

			using (var store = this.NewDocumentStore())
			{       
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Advanced.LuceneQuery<Blog>()
						.Where("Title.Length:3 AND Category:Rhinos")
						.WaitForNonStaleResultsAsOfNow().ToArray();

					Assert.Equal(1, results.Length);
					Assert.Equal("two", results[0].Title);
					Assert.Equal("Rhinos", results[0].Category);
				}
			}
		}

		[Fact]
		public void CanPerformDynamicQueryWithHighlightingUsingClientLuceneQuery()
		{
			var blogOne = new Blog
			{
				Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
				Category = "The Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "Target cras vitae felis arcu word.",
				Category = "Los Rhinos"
			};

			using (var store = this.NewDocumentStore())
			{
				string blogOneId;
				string blogTwoId;
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();

					blogOneId = s.Advanced.GetDocumentId(blogOne);
					blogTwoId = s.Advanced.GetDocumentId(blogTwo);
				}

				using (var s = store.OpenSession())
				{
					FieldHighlightings titleHighlightings;
					FieldHighlightings categoryHighlightings;

					var results = s.Advanced.LuceneQuery<Blog>()
						.Highlight("Title", 18, 2, out titleHighlightings)
						.Highlight("Category", 18, 2, out categoryHighlightings)
						.SetHighlighterTags("*","*")
						.Where("Title:(target word) OR Category:rhinos")
						.WaitForNonStaleResultsAsOfNow()
						.ToArray();

					Assert.Equal(3, results.Length);
					Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
					Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

					Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
					Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
				}
			}
		}

		[Fact]
		public void CanPerformDynamicQueryWithHighlighting()
		{
			var blogOne = new Blog
			{
				Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
				Category = "Ravens"
			};
			var blogTwo = new Blog
			{
				Title = "Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
				Category = "The Rhinos"
			};
			var blogThree = new Blog
			{
				Title = "Target cras vitae felis arcu word.",
				Category = "Los Rhinos"
			};

			using (var store = this.NewDocumentStore())
			{
				string blogOneId;
				string blogTwoId;
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();

					blogOneId = s.Advanced.GetDocumentId(blogOne);
					blogTwoId = s.Advanced.GetDocumentId(blogTwo);
				}

				using (var s = store.OpenSession())
				{
					FieldHighlightings titleHighlightings = null;
					FieldHighlightings categoryHighlightings = null;

					var results = s.Query<Blog>()
								   .Customize(
									   c =>
									   c.Highlight("Title", 18, 2, out titleHighlightings)
										.Highlight("Category", 18, 2, out categoryHighlightings)
										.SetHighlighterTags("*", "*")
										.WaitForNonStaleResultsAsOfNow())
								   .Search(x => x.Category, "rhinos")
								   .Search(x => x.Title, "target word")
								   .ToArray();

					Assert.Equal(3, results.Length);
					Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
					Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

					Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
					Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
				}
			}
		}

		[Fact]
		public void ExecutesQueryWithHighlightingsAgainstSimpleIndex()
		{
			using (var store = this.NewDocumentStore())
			{
				const string indexName = "BlogsForHighlightingTests";
				store.DatabaseCommands.PutIndex(indexName,
					new IndexDefinition
					{
						Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
						Stores =
						{
							{"Title", FieldStorage.Yes},
							{"Category", FieldStorage.Yes}
						},
						Indexes =
						{
							{"Title", FieldIndexing.Analyzed},
							{"Category", FieldIndexing.Analyzed}
						},
						TermVectors =
						{
							{"Title", FieldTermVector.WithPositionsAndOffsets},
							{"Category", FieldTermVector.WithPositionsAndOffsets}							
						}
					});

				var blogOne = new Blog
				{
					Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
					Category = "Ravens"
				};
				var blogTwo = new Blog
				{
					Title =
						"Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
					Category = "The Rhinos"
				};
				var blogThree = new Blog
				{
					Title = "Target cras vitae felis arcu word.",
					Category = "Los Rhinos"
				};

				string blogOneId;
				string blogTwoId;
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();

					blogOneId = s.Advanced.GetDocumentId(blogOne);
					blogTwoId = s.Advanced.GetDocumentId(blogTwo);
				}

				using (var s = store.OpenSession())
				{
					FieldHighlightings titleHighlightings = null;
					FieldHighlightings categoryHighlightings = null;

					var results = s.Query<Blog>(indexName)
								   .Customize(
									   c =>
									   c.Highlight("Title", 18, 2, out titleHighlightings)
										.Highlight("Category", 18, 2, out categoryHighlightings)
										.SetHighlighterTags("*", "*")
										.WaitForNonStaleResultsAsOfNow())
								   .Search(x => x.Category, "rhinos")
								   .Search(x => x.Title, "target word")
								   .ToArray();

					Assert.Equal(3, results.Length);
					Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
					Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

					Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
					Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
				}
			}
		}

		[Fact]
		public void ExecutesQueryWithHighlightingsAgainstMapReduceIndex()
		{
			using (var store = this.NewDocumentStore())
			{
				const string indexName = "BlogsForHighlightingMRTests";
				store.DatabaseCommands.PutIndex(indexName,
					new IndexDefinition
					{
						Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
						Reduce = @"from result in results 
								   group result by result.Category into g
								   select new { Category = g.Key, Title = g.Select(x=>x.Title).Aggregate(string.Concat) }",
						Stores =
						{
							{"Title", FieldStorage.Yes},
							{"Category", FieldStorage.Yes}
						},
						Indexes =
						{
							{"Title", FieldIndexing.Analyzed},
							{"Category", FieldIndexing.Analyzed}
						},
						TermVectors =
						{
							{"Title", FieldTermVector.WithPositionsAndOffsets},
							{"Category", FieldTermVector.WithPositionsAndOffsets}							
						}
					});

				var blogOne = new Blog
				{
					Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
					Category = "Ravens"
				};
				var blogTwo = new Blog
				{
					Title =
						"Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
					Category = "The Rhinos"
				};
				var blogThree = new Blog
				{
					Title = "Target cras vitae felis arcu word.",
					Category = "Los Rhinos"
				};

				string blogOneId;
				string blogTwoId;
				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();

					blogOneId = s.Advanced.GetDocumentId(blogOne);
					blogTwoId = s.Advanced.GetDocumentId(blogTwo);
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>(indexName)
								   .Customize(
									   c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
								   .Where(x => x.Title == "lorem" && x.Category == "ravens")
								   .Select(x => new
								   {
									   x.Title,
									   x.Category,
									   TitleFragments = default(string[])
								   })
								   .ToArray();

					Assert.Equal(1, results.Length);
					Assert.NotEmpty(results.First().TitleFragments);
				}
			}
		}

		[Fact]
		public void ExecutesQueryWithHighlightingsAndProjections()
		{
			using (var store = this.NewDocumentStore())
			{
				const string indexName = "BlogsForHighlightingTests";
				store.DatabaseCommands.PutIndex(indexName,
					new IndexDefinition
					{
						Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
						Stores =
						{
							{"Title", FieldStorage.Yes},
							{"Category", FieldStorage.Yes}
						},
						Indexes =
						{
							{"Title", FieldIndexing.Analyzed},
							{"Category", FieldIndexing.Analyzed}
						},
						TermVectors =
						{
							{"Title", FieldTermVector.WithPositionsAndOffsets},
							{"Category", FieldTermVector.WithPositionsAndOffsets}							
						}
					});

				var blogOne = new Blog
				{
					Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
					Category = "Ravens"
				};
				var blogTwo = new Blog
				{
					Title =
						"Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
					Category = "The Rhinos"
				};
				var blogThree = new Blog
				{
					Title = "Target cras vitae felis arcu word.",
					Category = "Los Rhinos"
				};

				using (var s = store.OpenSession())
				{
					s.Store(blogOne);
					s.Store(blogTwo);
					s.Store(blogThree);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Blog>(indexName)
								   .Customize(
									   c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
								   .Where(x => x.Title == "lorem" && x.Category == "ravens")
								   .Select(x => new
								   {
									   x.Title,
									   x.Category,
									   TitleFragments = default(string[])
								   })
								   .ToArray();

					Assert.Equal(1, results.Length);
					Assert.NotEmpty(results.First().TitleFragments);
				}
			}
		}
	   
		public class Blog
		{
			public string Title
			{
				get;
				set;
			}

			public string Category
			{
				get;
				set;
			}

			public BlogTag[] Tags
			{
				get;
				set;
			}
		}

		public class BlogTag
		{
			public string Name { get; set; }
		}
	}
}
