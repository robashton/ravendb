//-----------------------------------------------------------------------
// <copyright file="QueryBuilder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.LuceneIntegration;
using Version = Lucene.Net.Util.Version;
using System.Linq;

namespace Raven.Database.Indexing
{
	public static class QueryBuilder
	{
		private const string FieldRegexVal = @"([@\w\d<>_]+?):"; 
		static readonly Regex fieldQuery = new Regex(FieldRegexVal, RegexOptions.Compiled);
		static readonly Regex untokenizedQuery = new Regex( FieldRegexVal + @"[\s\(]*(\[\[.+?\]\])|(?<=,)\s*(\[\[.+?\]\])(?=\s*[,\)])", RegexOptions.Compiled);
		static readonly Regex searchQuery = new Regex(FieldRegexVal + @"\s*(\<\<.+?\>\>)(^[\d.]+)?", RegexOptions.Compiled | RegexOptions.Singleline);
		static readonly Regex dateQuery = new Regex(FieldRegexVal + @"\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z?)", RegexOptions.Compiled);

		private static readonly Dictionary<string, Func<string, List<string>, Query>> queryMethods = new Dictionary<string, Func<string, List<string>, Query>>(StringComparer.InvariantCultureIgnoreCase)
		{
			{"in", (field, args) => new TermsMatchQuery(field, args)},
			{"emptyIn", (field, args) => new TermsMatchQuery(field, args)}
		};

		public static Query BuildQuery(string query, RavenPerFieldAnalyzerWrapper analyzer)
		{
			return BuildQuery(query, new IndexQuery(), analyzer);
		}

		public static Query BuildQuery(string query, IndexQuery indexQuery, RavenPerFieldAnalyzerWrapper analyzer)
		{
			var originalQuery = query;
			try
			{
				var queryParser = new RangeQueryParser(Version.LUCENE_29, indexQuery.DefaultField ?? string.Empty, analyzer)
				{
					DefaultOperator = indexQuery.DefaultOperator == QueryOperator.Or
										? QueryParser.Operator.OR
										: QueryParser.Operator.AND,
					AllowLeadingWildcard = true
				};
				query = PreProcessUntokenizedTerms(query, queryParser);
				query = PreProcessSearchTerms(query);
				query = PreProcessDateTerms(query, queryParser);
				var generatedQuery = queryParser.Parse(query);
				generatedQuery = HandleMethods(generatedQuery, analyzer);
				return generatedQuery;
			}
			catch (ParseException pe)
			{
				if (originalQuery == query)
					throw new ParseException("Could not parse: '" + query + "'", pe);
				throw new ParseException("Could not parse modified query: '" + query + "' original was: '" + originalQuery + "'", pe);

			}
		}

		private static Query HandleMethods(Query query, RavenPerFieldAnalyzerWrapper analyzer)
		{
			var termQuery = query as TermQuery;
			if (termQuery != null && termQuery.Term.Field.StartsWith("@"))
			{
				return HandleMethodsForQueryAndTerm(query, termQuery.Term);
			}
			var pharseQuery = query as PhraseQuery;
			if (pharseQuery != null)
			{
				var terms = pharseQuery.GetTerms();
				if (terms.All(x => x.Field.StartsWith("@")) == false ||
				    terms.Select(x => x.Field).Distinct().Count() != 1)
					return query;
				return HandleMethodsForQueryAndTerm(query, terms);
			}
			var wildcardQuery = query as WildcardQuery;
			if (wildcardQuery != null)
			{
				return HandleMethodsForQueryAndTerm(query, wildcardQuery.Term);
			}
			var booleanQuery = query as BooleanQuery;
			if (booleanQuery != null)
			{
				foreach (var c in booleanQuery.Clauses)
				{
					c.Query = HandleMethods(c.Query, analyzer);
				}
				if (booleanQuery.Clauses.Count == 0)
					return booleanQuery;
			
				var mergeGroups = booleanQuery.Clauses.Select(x=>x.Query).OfType<IRavenLuceneMethodQuery>().GroupBy(x => x.Field).ToArray();
				if (mergeGroups.Length == 0)
					return booleanQuery;

				foreach (var mergeGroup in mergeGroups)
				{
					var clauses = mergeGroup.ToArray();
					var first = clauses[0];
					foreach (var mergedClause in clauses.Skip(1))
					{
						booleanQuery.Clauses.RemoveAll(x => ReferenceEquals(x.Query, mergedClause));
					}
					var ravenLuceneMethodQuery = clauses.Skip(1).Aggregate(first, (methodQuery, clause) => methodQuery.Merge(clause));
					booleanQuery.Clauses.First(x => ReferenceEquals(x.Query, first)).Query = (Query)ravenLuceneMethodQuery;
				}
				if (booleanQuery.Clauses.Count == 1)
					return booleanQuery.Clauses[0].Query;
				return booleanQuery;
			}
			return query;
		}

		private static Regex unescapedSplitter = new Regex("(?<!`),(?!`)", RegexOptions.Compiled);

		private static Query HandleMethodsForQueryAndTerm(Query query, Term term)
		{
			Func<string, List<string>, Query> value;
			var field = term.Field;
			if (TryHandlingMethodForQueryAndTerm(ref field, out value) == false) 
				return query;

			var parts = unescapedSplitter.Split(term.Text);
			var list = new List<string>(
					from part in parts
					where string.IsNullOrWhiteSpace(part) == false
					select part.Replace("`,`", ",")
			);
			return value(field, list);
		}

		private static Query HandleMethodsForQueryAndTerm(Query query, Term[] terms)
		{
			Func<string, List<string>, Query> value;
			var field = terms[0].Field;
			if (TryHandlingMethodForQueryAndTerm(ref field, out value) == false)
				return query;

			return value(field, terms.Select(x=>x.Text).ToList());
		}

		private static bool TryHandlingMethodForQueryAndTerm(ref string field,
			out Func<string, List<string>, Query> value)
		{
			value = null;
			var indexOfFieldStart = field.IndexOf('<');
			var indexOfFieldEnd = field.LastIndexOf('>');
			if (indexOfFieldStart == -1 || indexOfFieldEnd == -1)
			{
				return false;
			}
			var method = field.Substring(1, indexOfFieldStart - 1);
			field = field.Substring(indexOfFieldStart + 1, indexOfFieldEnd - indexOfFieldStart - 1);

			if (queryMethods.TryGetValue(method, out value) == false)
			{
				throw new InvalidOperationException("Method call " + field + " is invalid.");
			}
			return true;
		}

		private static string PreProcessDateTerms(string query, RangeQueryParser queryParser)
		{
			var searchMatches = dateQuery.Matches(query);
			if (searchMatches.Count < 1)
				return query;

			var queryStringBuilder = new StringBuilder(query);
			for (var i = searchMatches.Count - 1; i >= 0; i--) // reversing the scan so we won't affect positions of later items
			{
				var searchMatch = searchMatches[i];
				var field = searchMatch.Groups[1].Value;
				var termReplacement = searchMatch.Groups[2].Value;

				var replaceToken = queryParser.ReplaceToken(field, termReplacement);
				queryStringBuilder.Remove(searchMatch.Index, searchMatch.Length);
				queryStringBuilder
					.Insert(searchMatch.Index, field)
					.Insert(searchMatch.Index + field.Length, ":")
					.Insert(searchMatch.Index + field.Length + 1, replaceToken);
			}

			return queryStringBuilder.ToString();
		}

		private static string PreProcessSearchTerms(string query)
		{
			var searchMatches = searchQuery.Matches(query);
			if (searchMatches.Count < 1)
				return query;

			var queryStringBuilder = new StringBuilder(query);
			for (var i = searchMatches.Count - 1; i >= 0; i--) // reversing the scan so we won't affect positions of later items
			{
				var searchMatch = searchMatches[i];
				var field = searchMatch.Groups[1].Value;
				var terms = searchMatch.Groups[2].Value.Substring(2, searchMatch.Groups[2].Length - 4);

				string boost = "";
				if (string.IsNullOrWhiteSpace(searchMatch.Groups[3].Value) == false)
				{
					boost = searchMatch.Groups[3].Value;
				}

				queryStringBuilder.Remove(searchMatch.Index, searchMatch.Length);
				queryStringBuilder.Insert(searchMatch.Index, '(');
				var len = searchMatch.Index;
				foreach (var term in terms.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
				{
					switch (term) // ignore invalid options
					{
						case "OR":
						case "AND":
						case "||":
						case "&&":
							continue;
					}

					var termQuery = new StringBuilder().Append(field).Append(':').Append(term).Append(boost).Append(' ');
					len += termQuery.Length;
					queryStringBuilder.Insert(searchMatch.Index + 1, termQuery);
				}
				queryStringBuilder.Insert(len, ')');
			}
			return queryStringBuilder.ToString();
		}

		/// <summary>
		/// Detects untokenized fields and sets as NotAnalyzed in analyzer
		/// </summary>
		private static string PreProcessUntokenizedTerms(string query, RangeQueryParser queryParser)
		{
			var untokenizedMatches = untokenizedQuery.Matches(query);
			if (untokenizedMatches.Count < 1)
				return query;

			var sb = new StringBuilder(query);
			MatchCollection fieldMatches = null;

			// process in reverse order to leverage match string indexes
			for (var i = untokenizedMatches.Count; i > 0; i--)
			{
				var match = untokenizedMatches[i - 1];

				// specify that term for this field should not be tokenized
				var value = match.Groups[2].Value;
				var term = match.Groups[2];
				string name = match.Groups[1].Value;
				if (string.IsNullOrEmpty(value))
				{
					value = match.Groups[3].Value;
					term = match.Groups[3];
					if(fieldMatches == null)
						fieldMatches = fieldQuery.Matches(query);

					var lastField = fieldMatches.Cast<Match>().LastOrDefault(x => x.Index <= term.Index);
					if (lastField != null)
					{
						name = lastField.Groups[1].Value;
					}
				}
				var rawTerm = value.Substring(2, value.Length - 4);
				queryParser.SetUntokenized(name, Unescape(rawTerm));


				// introduce " " around the term
				var startIndex = term.Index;
				var length = term.Length - 2;
				if (sb[startIndex + length - 1] != '"')
				{
					sb.Insert(startIndex + length, '"');
					length += 1;
				}
				if (sb[startIndex + 2] != '"')
				{
					sb.Insert(startIndex + 2, '"');
					length += 1;
				}
				// remove enclosing "[[" "]]" from term value (again in reverse order)
				sb.Remove(startIndex + length, 2);
				sb.Remove(startIndex, 2);
			}

			return sb.ToString();
		}

		public static string Unescape(string term)
		{
			// method doesn't allocate a StringBuilder unless the string requires unescaping
			// also this copies chunks of the original string into the StringBuilder which
			// is far more efficient than copying character by character because StringBuilder
			// can access the underlying string data directly

			if (string.IsNullOrEmpty(term))
			{
				return term;
			}

			bool isPhrase = term.StartsWith("\"") && term.EndsWith("\"");
			int start = 0;
			int length = term.Length;
			StringBuilder buffer = null;
			char prev = '\0';
			for (int i = start; i < length; i++)
			{
				char ch = term[i];
				if (prev != '\\')
				{
					prev = ch;
					continue;
				}
				prev = ch;
				switch (ch)
				{
					case '*':
					case '?':
					case '+':
					case '-':
					case '&':
					case '|':
					case '!':
					case '(':
					case ')':
					case '{':
					case '}':
					case '[':
					case ']':
					case '^':
					case '"':
					case '~':
					case ':':
					case '\\':
						{
							if (buffer == null)
							{
								// allocate builder with headroom
								buffer = new StringBuilder(length * 2);
							}
							// append any leading substring
							buffer.Append(term, start, i - start - 1);

							buffer.Append(ch);
							start = i + 1;
							break;
						}
				}
			}

			if (buffer == null)
			{
				if (isPhrase)
					return term.Substring(1, term.Length - 2);
				// no changes required
				return term;
			}

			if (length > start)
			{
				// append any trailing substring
				buffer.Append(term, start, length - start);
			}

			return buffer.ToString();
		}
	}
}
