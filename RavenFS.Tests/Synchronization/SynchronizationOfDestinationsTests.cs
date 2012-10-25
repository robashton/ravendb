namespace RavenFS.Tests.Synchronization
{
	using System;
	using System.Collections.Generic;
	using System.Collections.Specialized;
	using System.IO;
	using System.Linq;
	using System.Text;
	using IO;
	using Newtonsoft.Json;
	using RavenFS.Client;
	using RavenFS.Extensions;
	using RavenFS.Notifications;
	using RavenFS.Synchronization;
	using RavenFS.Util;
	using Xunit;

	public class SynchronizationOfDestinationsTests : MultiHostTestBase
	{
		private const int AddtitionalServerInstancePortNumber = 19083;

		[Fact]
		public void Should_synchronize_to_all_destinations()
		{
			StartServerInstance(AddtitionalServerInstancePortNumber);

			var sourceContent = SyncTestUtils.PrepareSourceStream(10000);
			sourceContent.Position = 0;

			var sourceClient = NewClient(0);

			var destination1Client = NewClient(1);
			var destination2Client = new RavenFileSystemClient(ServerAddress(AddtitionalServerInstancePortNumber));

			var destination1Content = new RandomlyModifiedStream(sourceContent, 0.01);
			sourceContent.Position = 0;
			var destination2Content = new RandomlyModifiedStream(sourceContent, 0.01);
			sourceContent.Position = 0;

			destination1Client.UploadAsync("test.bin", destination1Content).Wait();
			destination2Client.UploadAsync("test.bin", destination2Content).Wait();

			sourceContent.Position = 0;
			sourceClient.UploadAsync("test.bin", sourceContent).Wait();
			sourceContent.Position = 0;

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			// we expect conflicts after first attempt of synchronization
			Assert.Equal(2, destinationSyncResults.Length);
			Assert.Equal("File test.bin is conflicted", destinationSyncResults[0].Reports.ToArray()[0].Exception.Message);
			Assert.Equal("File test.bin is conflicted", destinationSyncResults[1].Reports.ToArray()[0].Exception.Message);

			destination1Client.Synchronization.ResolveConflictAsync("test.bin", ConflictResolutionStrategy.RemoteVersion).Wait();
			destination2Client.Synchronization.ResolveConflictAsync("test.bin", ConflictResolutionStrategy.RemoteVersion).Wait();

			destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			// check if reports match
			Assert.Equal(2, destinationSyncResults.Length);
			var result1 = destinationSyncResults[0].Reports.ToArray()[0];
			Assert.Equal(sourceContent.Length, result1.BytesCopied + result1.BytesTransfered);

			var result2 = destinationSyncResults[1].Reports.ToArray()[0];
			Assert.Equal(sourceContent.Length, result2.BytesCopied + result2.BytesTransfered);

			// check content of files
			string destination1Md5 = null;
			using (var resultFileContent = new MemoryStream())
			{
				destination1Client.DownloadAsync("test.bin", resultFileContent).Wait();
				resultFileContent.Position = 0;
				destination1Md5 = IOExtensions.GetMD5Hash(resultFileContent);
			}

			string destination2Md5 = null;
			using (var resultFileContent = new MemoryStream())
			{
				destination2Client.DownloadAsync("test.bin", resultFileContent).Wait();
				resultFileContent.Position = 0;
				destination2Md5 = resultFileContent.GetMD5Hash();
			}

			sourceContent.Position = 0;
			var sourceMd5 = sourceContent.GetMD5Hash();

			Assert.Equal(sourceMd5, destination1Md5);
			Assert.Equal(sourceMd5, destination2Md5);
			Assert.Equal(destination1Md5, destination2Md5);
		}

		[Fact]
		public void Should_not_synchronize_file_back_to_source_if_origins_from_source()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", new RandomStream(1024)).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			// synchronize from source to destination
			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			Assert.Equal(1, destinationSyncResults[0].Reports.Count());
			Assert.Equal(SynchronizationType.ContentUpdate, destinationSyncResults[0].Reports.ToArray()[0].Type);

			destinationClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", sourceClient.ServerUrl }
			                                                                                     	}).Wait();

			// synchronize from destination to source
			var sourceSyncResults = destinationClient.Synchronization.SynchronizeDestinationsAsync().Result;

			Assert.Equal(1, sourceSyncResults.Count());
			Assert.Null(sourceSyncResults[0].Reports);
		}

		[Fact]
		public void Synchronization_should_upload_all_missing_files()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			var source1Content = new RandomStream(10000);

			sourceClient.UploadAsync("test1.bin", source1Content).Wait();
			
			var source2Content = new RandomStream(10000);

			sourceClient.UploadAsync("test2.bin", source2Content).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var destinationFiles = destinationClient.GetFilesAsync("/").Result;
			Assert.Equal(2, destinationFiles.FileCount);
			Assert.Equal(2, destinationFiles.Files.Length);
			Assert.NotEqual(destinationFiles.Files[0].Name, destinationFiles.Files[1].Name);
			Assert.True(destinationFiles.Files[0].Name == "test1.bin" || destinationFiles.Files[0].Name == "test2.bin");
			Assert.True(destinationFiles.Files[1].Name == "test1.bin" || destinationFiles.Files[1].Name == "test2.bin");
		}

		[Fact]
		public void Make_sure_that_locks_are_released_after_synchronization_when_two_files_synchronized_simultaneously()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			var source1Content = new RandomStream(10000);

			sourceClient.UploadAsync("test1.bin", source1Content).Wait();

			var source2Content = new RandomStream(10000);

			sourceClient.UploadAsync("test2.bin", source2Content).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var configs = destinationClient.Config.GetConfigNames().Result;

			Assert.DoesNotContain("SyncingLock-test1.bin", configs);
			Assert.DoesNotContain("SyncingLock-test2.bin", configs);

			// also make sure that results exist
			Assert.Contains("SyncResult-test1.bin", configs);
			Assert.Contains("SyncResult-test2.bin", configs);
		}

		[Fact]
		public void Source_should_save_configuration_record_after_synchronization()
		{
			var sourceClient = NewClient(0);
			var sourceContent = new RandomStream(10000);

			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var savedRecord =
				sourceClient.Config.GetConfig(RavenFileNameHelper.SyncNameForFile("test.bin", destinationClient.ServerUrl)).Result
					["value"];

			var synchronizationDetails = new TypeHidingJsonSerializer().Parse<SynchronizationDetails>(savedRecord);

			Assert.Equal("test.bin", synchronizationDetails.FileName);
			Assert.Equal(destinationClient.ServerUrl, synchronizationDetails.DestinationUrl);
			Assert.NotEqual(Guid.Empty, synchronizationDetails.FileETag);
			Assert.Equal(SynchronizationType.ContentUpdate, synchronizationDetails.Type);
		}

		[Fact]
		public void Source_should_delete_configuration_record_if_destination_confirm_that_file_is_safe()
		{
			var sourceClient = NewClient(0);
			var sourceContent = new RandomStream(10000);

			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();


			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();
			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();
			
			// start synchronization again to force confirmation by source
			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait(); 

			var shouldBeNull =
				sourceClient.Config.GetConfig(RavenFileNameHelper.SyncNameForFile("test.bin", destinationClient.ServerUrl)).Result;

			Assert.Null(shouldBeNull);
		}

		[Fact]
		public void File_should_be_in_pending_queue_if_no_synchronization_requests_available()
		{
			var sourceContent = new RandomStream(1);
			var sourceClient = NewClient(0);

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationLimit,
										  new NameValueCollection { { "value", "\"1\"" } }).Wait();

			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();
			sourceClient.UploadAsync("test2.bin", sourceContent).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();
			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var pendingSynchronizations = sourceClient.Synchronization.GetPendingAsync().Result;
			
			Assert.Equal(1, pendingSynchronizations.TotalCount);
			Assert.Equal(destinationClient.ServerUrl, pendingSynchronizations.Items[0].DestinationUrl);
		}

		[Fact]
		public void Should_change_metadata_on_all_destinations()
		{
			StartServerInstance(AddtitionalServerInstancePortNumber);

			var sourceClient = NewClient(0);

			var destination1Client = NewClient(1);
			var destination2Client = new RavenFileSystemClient(ServerAddress(AddtitionalServerInstancePortNumber));

			var sourceContent = new MemoryStream();
			var streamWriter = new StreamWriter(sourceContent);
			var expected = new string('a', 1024 * 1024 * 10);
			streamWriter.Write(expected);
			streamWriter.Flush();
			sourceContent.Position = 0;

			sourceClient.UploadAsync("test.txt", sourceContent).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			// push file to all destinations
			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			// prevent pushing files after metadata update
			sourceClient.Config.DeleteConfig(SynchronizationConstants.RavenSynchronizationDestinations).Wait();

			sourceClient.UpdateMetadataAsync("test.txt", new NameValueCollection() { { "value", "shouldBeSynchronized" } }).Wait();

			// add destinations again
			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			// should synchronize metadata
			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			foreach (var destinationSyncResult in destinationSyncResults)
			{
				foreach (var report in destinationSyncResult.Reports)
				{
					Assert.Null(report.Exception);
					Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);
				}
			}

			Assert.Equal("shouldBeSynchronized", destination1Client.GetMetadataForAsync("test.txt").Result["value"]);
			Assert.Equal("shouldBeSynchronized", destination2Client.GetMetadataForAsync("test.txt").Result["value"]);
		}
		
		[Fact]
		public void Should_rename_file_on_all_destinations()
		{
			StartServerInstance(AddtitionalServerInstancePortNumber);

			var sourceClient = NewClient(0);

			var destination1Client = NewClient(1);
			var destination2Client = new RavenFileSystemClient(ServerAddress(AddtitionalServerInstancePortNumber));

			// upload file to all servers
			sourceClient.UploadAsync("test.bin", new RandomStream(10)).Wait();
			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			sourceClient.Config.DeleteConfig(SynchronizationConstants.RavenSynchronizationDestinations).Wait();
			

			// delete file on source
			sourceClient.RenameAsync("test.bin", "rename.bin").Wait();

			// set up destinations
			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			foreach (var destinationSyncResult in destinationSyncResults)
			{
				foreach (var report in destinationSyncResult.Reports)
				{
					Assert.Null(report.Exception);
					Assert.Equal(SynchronizationType.Rename, report.Type);
				}
			}

			Assert.Null(destination1Client.GetMetadataForAsync("test.bin").Result);
			Assert.Null(destination2Client.GetMetadataForAsync("test.bin").Result);
			Assert.NotNull(destination1Client.GetMetadataForAsync("rename.bin").Result);
			Assert.NotNull(destination2Client.GetMetadataForAsync("rename.bin").Result);
		}

		[Fact]
		public void Should_delete_file_on_all_destinations()
		{
			StartServerInstance(AddtitionalServerInstancePortNumber);

			var sourceClient = NewClient(0);

			var destination1Client = NewClient(1);
			var destination2Client = new RavenFileSystemClient(ServerAddress(AddtitionalServerInstancePortNumber));

			// upload file to first server and synchronize to others
			sourceClient.UploadAsync("test.bin", new RandomStream(10)).Wait();
			sourceClient.Synchronization.StartSynchronizationToAsync("test.bin", destination1Client.ServerUrl).Wait();
			sourceClient.Synchronization.StartSynchronizationToAsync("test.bin", destination2Client.ServerUrl).Wait();

			// delete file on source
			sourceClient.DeleteAsync("test.bin").Wait();

			// set up destinations
			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destination1Client.ServerUrl },
																										{ "url", destination2Client.ServerUrl }
			                                                                                     	}).Wait();

			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			foreach (var destinationSyncResult in destinationSyncResults)
			{
				foreach (var report in destinationSyncResult.Reports)
				{
					Assert.Null(report.Exception);
					Assert.Equal(SynchronizationType.Delete, report.Type);
				}
			}

			Assert.Null(destination1Client.GetMetadataForAsync("test.bin").Result);
			Assert.Null(destination1Client.GetMetadataForAsync("test.bin").Result);
		}

		[Fact]
		public void Should_confirm_that_file_is_safe()
		{
			var sourceContent = new RandomStream(1024 * 1024);

			var sourceClient = NewClient(1);
			var destinationClient = NewClient(0);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var confirmations = destinationClient.Synchronization.ConfirmFilesAsync(new List<Tuple<string, Guid>> { new Tuple<string, Guid>("test.bin", sourceClient.GetMetadataForAsync("test.bin").Result.Value<Guid>("ETag"))}).Result;

			Assert.Equal(1, confirmations.Count());
			Assert.Equal(FileStatus.Safe, confirmations.ToArray()[0].Status);
			Assert.Equal("test.bin", confirmations.ToArray()[0].FileName);
		}

		[Fact]
		public void Should_say_that_file_status_is_unknown_if_there_is_different_etag()
		{
			var sourceContent = new RandomStream(1024 * 1024);

			var sourceClient = NewClient(1);
			var destinationClient = NewClient(0);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			sourceClient.Synchronization.SynchronizeDestinationsAsync().Wait();

			var differentETag = Guid.NewGuid();

			var confirmations = destinationClient.Synchronization.ConfirmFilesAsync(new List<Tuple<string, Guid>> { new Tuple<string, Guid>("test.bin", differentETag) }).Result;

			Assert.Equal(1, confirmations.Count());
			Assert.Equal(FileStatus.Unknown, confirmations.ToArray()[0].Status);
			Assert.Equal("test.bin", confirmations.ToArray()[0].FileName);
		}

		[Fact]
		public void Should_report_that_file_state_is_unknown_if_file_doesnt_exist()
		{
			var destinationClient = NewClient(0);

			var confirmations = destinationClient.Synchronization.ConfirmFilesAsync(new List<Tuple<string, Guid>> { new Tuple<string, Guid>("test.bin", Guid.Empty) }).Result;

			Assert.Equal(1, confirmations.Count());
			Assert.Equal(FileStatus.Unknown, confirmations.ToArray()[0].Status);
			Assert.Equal("test.bin", confirmations.ToArray()[0].FileName);
		}
		
		[Fact]
		public void Should_report_that_file_is_broken_if_last_synchronization_set_exception()
		{
			var destinationClient = NewClient(0);

			var sampleGuid = Guid.Empty;

			var failureSynchronization = new SynchronizationReport("test.bin",  sampleGuid, SynchronizationType.Unknown)
			                             	{Exception = new Exception("There was an exception in last synchronization.")};

			var sb = new StringBuilder();
            var jw = new JsonTextWriter(new StringWriter(sb));
            new JsonSerializer().Serialize(jw, failureSynchronization);

			destinationClient.Config.SetConfig(RavenFileNameHelper.SyncResultNameForFile("test.bin"),
			                                   new NameValueCollection() {{"value", sb.ToString()}}).Wait();

			var confirmations = destinationClient.Synchronization.ConfirmFilesAsync(new List<Tuple<string, Guid>> { new Tuple<string, Guid>("test.bin", sampleGuid) }).Result;

			Assert.Equal(1, confirmations.Count());
			Assert.Equal(FileStatus.Broken, confirmations.ToArray()[0].Status);
			Assert.Equal("test.bin", confirmations.ToArray()[0].FileName);
		}

		[Fact]
		public void Should_not_synchronize_if_file_is_conflicted_on_destination()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			destinationClient.UploadAsync("file.bin",
			                              new NameValueCollection
			                              	{{SynchronizationConstants.RavenSynchronizationConflict, "true"}},
			                              new RandomStream(10)).Wait();

			sourceClient.UploadAsync("file.bin", new RandomStream(10)).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			Assert.Null(destinationSyncResults[0].Reports);
		}

		[Fact]
		public void Should_not_synchronize_if_file_is_conflicted_on_source()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("file.bin",
			                         new NameValueCollection {{SynchronizationConstants.RavenSynchronizationConflict, "true"}},
			                         new RandomStream(10)).Wait();

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			var destinationSyncResults = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			Assert.Null(destinationSyncResults[0].Reports);
		}

		[Fact]
		public void Should_not_fail_if_no_destinations_given()
		{
			var sourceClient = NewClient(0);

			IEnumerable<DestinationSyncResult> results = null;

			Assert.DoesNotThrow(() => results = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result);
			Assert.Equal(0, results.Count());
		}

		[Fact]
		public void Should_not_fail_if_there_is_no_file_to_synchronize()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationDestinations, new NameValueCollection
			                                                                                     	{
			                                                                                     		{ "url", destinationClient.ServerUrl }
			                                                                                     	}).Wait();

			IEnumerable<DestinationSyncResult> results = null;

			Assert.DoesNotThrow(() => results = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result);

			Assert.Null(results.ToArray()[0].Reports);
		}
	}
}