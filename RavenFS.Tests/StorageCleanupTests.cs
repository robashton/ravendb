﻿namespace RavenFS.Tests
{
	using System;
	using System.IO;
	using Newtonsoft.Json;
	using Storage;
	using Synchronization;
	using Synchronization.IO;
	using Util;
	using Xunit;

	public class StorageCleanupTests : WebApiTest
	{
		[Fact]
		public void Should_create_apropriate_config_after_file_delete()
		{
			var client = NewClient();
			client.UploadAsync("toDelete.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			var config =
				client.Config.GetConfig(
					RavenFileNameHelper.DeletingFileConfigNameForFile(RavenFileNameHelper.DeletingFileName("toDelete.bin")))
					.Result;

			Assert.NotNull(config);

			var deleteFile = new JsonSerializer().Deserialize<DeleteFile>(new JsonTextReader(new StringReader(config["value"])));

			Assert.Equal(RavenFileNameHelper.DeletingFileName("toDelete.bin"), deleteFile.CurrentFileName);
			Assert.Equal("toDelete.bin", deleteFile.OriginalFileName);
		}

		[Fact]
		public void Should_remove_file_deletion_config_after_storage_cleanup()
		{
			var client = NewClient();
			client.UploadAsync("toDelete.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 })).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			client.Storage.CleanUp().Wait();

			var configNames = client.Config.GetConfigNames().Result;

			Assert.DoesNotContain(RavenFileNameHelper.DeletingFileConfigNameForFile(RavenFileNameHelper.DeletingFileName("toDelete.bin")), configNames);
		}

		[Fact]
		public void Should_remove_deleting_file_and_its_pages_after_storage_cleanup()
		{
			const int numberOfPages = 10;

			var client = NewClient();

			var bytes = new byte[numberOfPages * StorageConstants.MaxPageSize];
			new Random().NextBytes(bytes);

			client.UploadAsync("toDelete.bin", new MemoryStream(bytes)).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			client.Storage.CleanUp().Wait();

			var storage = GetRavenFileSystem().Storage;

			Assert.Throws(typeof (FileNotFoundException),
			              () =>
			              storage.Batch(accessor => accessor.GetFile(RavenFileNameHelper.DeletingFileName("toDelete.bin"), 0, 10)));

			for (int i = 1; i <= numberOfPages; i++)
			{
				int pageId = 0;
				storage.Batch(accessor => pageId = accessor.ReadPage(i, null));
				Assert.Equal(-1, pageId); // if page does not exist we return -1
			}
		}

		[Fact]
		public void Should_not_perform_file_delete_if_it_is_being_synced()
		{
			var client = NewClient();

			client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 })).Wait();

			client.DeleteAsync("file.bin").Wait();

			client.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("file.bin"), LockFileTests.SynchronizationConfig(DateTime.UtcNow)).Wait();

			client.Storage.CleanUp().Wait();

			var config =
				client.Config.GetConfig(
					RavenFileNameHelper.DeletingFileConfigNameForFile(RavenFileNameHelper.DeletingFileName("file.bin")))
					.Result;

			// config should still exists
			Assert.NotNull(config);

			var deleteFile = new JsonSerializer().Deserialize<DeleteFile>(new JsonTextReader(new StringReader(config["value"])));

			Assert.Equal(RavenFileNameHelper.DeletingFileName("file.bin"), deleteFile.CurrentFileName);
			Assert.Equal("file.bin", deleteFile.OriginalFileName);
		}

		[Fact]
		public void Should_not_perform_file_delete_of_previous_downloading_file_if_synchronization_is_being_performed()
		{
			var fileName = "file.bin";
			var downloadingFileName = RavenFileNameHelper.DownloadingFileName(fileName);

			var client = NewClient();

			client.UploadAsync(fileName, new RandomStream(1)).Wait();
			
			client.UploadAsync(downloadingFileName, new RandomStream(1)).Wait();

			client.DeleteAsync(downloadingFileName).Wait();

			client.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile(fileName), LockFileTests.SynchronizationConfig(DateTime.UtcNow)).Wait();

			client.Storage.CleanUp().Wait();

			var config =
				client.Config.GetConfig(
					RavenFileNameHelper.DeletingFileConfigNameForFile(RavenFileNameHelper.DeletingFileName(downloadingFileName)))
					.Result;

			// config should still exists
			Assert.NotNull(config);

			var deleteFile = new JsonSerializer().Deserialize<DeleteFile>(new JsonTextReader(new StringReader(config["value"])));

			Assert.Equal(RavenFileNameHelper.DeletingFileName(downloadingFileName), deleteFile.CurrentFileName);
			Assert.Equal(downloadingFileName, deleteFile.OriginalFileName);
		}
	}
}