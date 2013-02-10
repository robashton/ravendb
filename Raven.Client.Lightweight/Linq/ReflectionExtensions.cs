//-----------------------------------------------------------------------
// <copyright file="ReflectionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Reflection;

namespace Raven.Client.Linq
{
	internal static class ReflectionExtensions
	{
		public static Type GetMemberType(this MemberInfo member)
		{
			switch (member.MemberType)
			{
				case MemberTypes.Field:
					return ((FieldInfo)member).FieldType;
				case MemberTypes.Property:
					return ((PropertyInfo)member).PropertyType;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
	}
}