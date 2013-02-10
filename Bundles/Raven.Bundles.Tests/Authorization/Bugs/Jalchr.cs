//-----------------------------------------------------------------------
// <copyright file="Jalchr.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;

using System.Collections.Generic;
using Xunit;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
	public class Jalchr : AuthorizationTest
	{
		[Fact]
		public void WithStandardUserName()
		{
			var userId = "Users/Ayende";
			ExecuteSecuredOperation(userId);
		}

		[Fact]
		public void WithRavenPrefixUserName()
		{
			var userId = "Raven/Users/Ayende";
			ExecuteSecuredOperation(userId);
		}

		private void ExecuteSecuredOperation(string userId)
		{
			string operation = "operation";
			using (var s = store.OpenSession())
			{
				AuthorizationUser user = new AuthorizationUser { Id = userId, Name = "Name" };
				user.Permissions = new List<OperationPermission>
				{
					new OperationPermission {Allow = true, Operation = operation}
				};
				s.Store(user);

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				var authorizationUser = s.Load<AuthorizationUser>(userId);
				Assert.True(AuthorizationClientExtensions.IsAllowed(s, authorizationUser, operation));
			}
		}
	}
}