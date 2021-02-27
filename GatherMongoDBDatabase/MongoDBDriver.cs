using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GatherCommon;
using GatherCommon.dtos;
using GatherCommon.dtos.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace GatherMongoDBDatabase
{
    // ReSharper disable once InconsistentNaming
    public class MongoDBDriver : IMongoDBDriver
    {
        private const string MongoLoggingoDbName = "logs";
        private const string MongoLoggingServerLogCollection = "serverlogs";
        private const string MongoLoggingApplicationLogCollection = "applicationlogs";
        private const string MongoDbName = "gatherapp";
        private const string MongoDbImageQueueCollection = "imagequeue";
        private readonly IMongoDatabase _lastGatherAppDatabase;
        private readonly IMongoDatabase _lastLoggingDatabase1;
        private readonly string _mongoDBConnectionString;
        private readonly string _mongoDBConnectionStringLogging;
        private static IMongoDBDriver _instance = null;

        public static IMongoDBDriver GetInstance(List<string> mongoDbConnectionStrings, string mongoDbConnectionStringLogging)
        {
            if (_instance == null)
            {
                _instance = new MongoDBDriver(mongoDbConnectionStrings, mongoDbConnectionStringLogging);
            }
            return _instance;
        }

        private IMongoClient CheckAwsDBMongoContext(string mongoDbConnectionString)
        {
            var uri = new Uri(mongoDbConnectionString);
            var userInfo = uri.UserInfo.Split(':');

            MongoClientSettings settings;

            if (userInfo == null || userInfo.Length <= 1)
            {
                var host = uri.Host;
                var port = uri.Port;

                settings = new MongoClientSettings
                {
                    Server = new MongoServerAddress(host, port),
                };
            }
            else
            {
                var username = userInfo[0];
                var password = userInfo[1];
                var host = uri.Host;
                var port = uri.Port;

                MongoCredential credential = MongoCredential.CreateCredential("admin", username, password);
                settings = new MongoClientSettings
                {
                    Credential = credential,
                    Server = new MongoServerAddress(host, port),
                };
            }

            var client = new MongoClient(settings);
            return client;
        }

        private MongoDBDriver(List<string> mongoDbConnectionStrings, string mongoDbConnectionStringLogging)
        {
            foreach (var mongoDbConnectionString in mongoDbConnectionStrings)
            {
                _mongoDBConnectionString = mongoDbConnectionString;
                _mongoDBConnectionStringLogging = mongoDbConnectionStringLogging;
                BsonDefaults.GuidRepresentation = GuidRepresentation.Standard;
                try
                {
                    var client = CheckAwsDBMongoContext(mongoDbConnectionString);
                    _lastGatherAppDatabase = client.GetDatabase(MongoDbName);

                    // Make sure database is there
                    GetDisallowedWords();
                    break;
                }
                catch (Exception exp)
                {
                    continue;
                }
            }
        }

        public async Task<long> CleanupGatheredPhotosAsync(int minDaysBack = 14)
        {
            var numberDeleted = await CleanupGatheredPhotosThatAreOlderThenAsync(TimeSpan.FromDays(minDaysBack));
            return numberDeleted;
        }

        public async Task<long> CleanupAllInformationAsync(int minDaysBack = 5)
        {
            long deletedCount = 0;

            deletedCount += await CleanupGatheredPhotosThatAreOlderThenAsync(TimeSpan.FromDays(minDaysBack));
            deletedCount += await CleanupSearchInformationResultsThatAreOlderThenAsync(TimeSpan.FromDays(minDaysBack));
            deletedCount += await CleanupImageQueueThatAreOlderThenAsync(TimeSpan.FromDays(minDaysBack));
            return deletedCount;
        }

        public async Task LogToMongoDbAsync(string collectionName, LoggingInformationDto logEntry)
        {
            try
            {
                var clientLogging = new MongoClient(_mongoDBConnectionStringLogging);
                var lastLoggingDatabase = clientLogging.GetDatabase(MongoLoggingoDbName);
                var searchInformationCollection =
                    lastLoggingDatabase.GetCollection<LoggingInformationDto>(collectionName);
                searchInformationCollection.InsertOne(logEntry);
            }
            catch (Exception exp)
            {
                Console.WriteLine($"Ignoring logging exception {exp.Message}");
            }
        }

        public async Task<List<SearchInformationResultsDto>> GetSearchEntriesThatAreOlderThenAsync(TimeSpan numberOfDaysBack)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>("searchInformationResults");

            var dateTimeString = DateTime.Now.Subtract(numberOfDaysBack).Date.ToString("yyyy-MM-dd");
            var filterString = $"{{ LastGatheredDateTime: {{$lt: \"{dateTimeString}\"}}}}";
            try
            {
                var elementList = searchInformationCollection.Find<SearchInformationResultsDto>(filterString);
                var count = elementList.CountDocuments();
                return null;
            }
            catch (Exception exp)
            {
                Console.WriteLine($"We got an exception {exp.Message}");
                throw;
            }
        }

        public async Task<long> CleanupSearchInformationResultsThatAreOlderThenAsync(TimeSpan numberOfDaysBack)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>("searchInformationResults");
            var dateTimeString = DateTime.Now.Subtract(numberOfDaysBack).Date.ToString("yyyy-MM-dd");
            var filterString = $"{{ LastGatheredDateTime: {{$lt: '{dateTimeString}'}}}}";
            var filterString2 = $"{{}}";
            var elementList = searchInformationCollection.Find<SearchInformationResultsDto>(filterString); 
            var count2 = elementList.Count();
            var ajr = elementList.FirstOrDefault();

            var result = searchInformationCollection.DeleteMany(filterString);
            return result.DeletedCount;
        }

        private async Task<List<T>> ExecuteQueryAsync<T>(IMongoCollection<T> searchCollection, string filterString)
        {
            try
            {
                var elementList = searchCollection.Find<T>(filterString);
                return elementList.ToList();
            }
            catch (Exception exp)
            {
                Console.WriteLine($"We got an exception {exp.Message}");
                throw;
            }
        }

        public async Task<List<SearchInformationResultsDto>> GetSearchInformationResultsAsync(string searchText,
            bool previousDay, bool previousWeek, bool safeSearch, Sizes sizeSearch)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>("searchInformationResults");

            var filterString =
                GetSearchInformationFilterString(searchText, previousDay, previousWeek, safeSearch, sizeSearch);

            try
            {
                var elementList = searchInformationCollection.Find<SearchInformationResultsDto>(filterString);
                var retValue = elementList.ToList();
                return retValue;
            }
            catch (Exception exp)
            {
                Console.WriteLine($"We got an exception {exp.Message}");
                throw;
            }
        }

        private string GetSearchInformationFilterString(string searchText, bool previousDay, bool previousWeek,
            bool safeSearch,
            Sizes sizeSearch)
        {
            var filterString =
                $"{{ $and: [{{\"SearchText\" : \"{searchText.Replace("\"", "\\\"")}\"}},";
            filterString +=
                $" {{\"PreviousDay\" : {previousDay.ToString().ToLower()}}}, {{\"PreviousWeek\" : {previousWeek.ToString().ToLower()}}}, {{\"SafeSearch\" : {safeSearch.ToString().ToLower()}}}, {{ $or: [ {{ \"SizeSearch\": {(int) sizeSearch} }} , {{\"SizeSearch\": \"{sizeSearch.ToString()}\"}}]}}] }}";
            return filterString;
        }

        private string GetCommandQueueInformationFilterString(CommandQueueInformationDto cqinfo)
        {
            var filterString = "{}";
            bool addHeaderDone = false;
            var seperator = "";

            if (cqinfo.GuidString != null)
            {
                return $"{{\"GuidString\":'{cqinfo.GuidString}'}}";
            }

            if (cqinfo.BaseURL != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                var url = cqinfo.BaseURL.Replace("/", "\\/");
                url.Replace("'", "\\'");


                filterString += $"{{BaseURL: {{$eq: '{url}'}}}}";
                seperator = ",";
            }

            if (cqinfo.UserID != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString += AddFilterElementToString("UserID", cqinfo.UserID, seperator, true, "$eq", false);
                seperator = ",";
            }
            else if (cqinfo.UserName != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString += AddFilterElementToString("UserName", cqinfo.UserName, seperator, true, "$eq", false);
                seperator = ",";
            }
            else if (cqinfo.GroupName != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                if (cqinfo.GroupName.Contains("'"))
                {
                    Console.WriteLine("yes");
                }

                filterString += AddFilterElementToString("GroupName", cqinfo.GroupName, seperator, true, "$eq", false);
                seperator = ",";
            }
            else 
            if (cqinfo.AlbumID != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString += AddFilterElementToString("AlbumID", cqinfo.AlbumID, seperator, true, "$eq", false);
                seperator = ",";
            }
            else if (cqinfo.GroupID != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString += AddFilterElementToString("GroupID", cqinfo.GroupID, seperator, true, "$eq", false);
                seperator = ",";
            }
            else if (cqinfo.searchString != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString +=
                    AddFilterElementToString("searchString", cqinfo.searchString, seperator, true, "$eq", false);
                seperator = ",";
            }

            if (!addHeaderDone)
            {
                filterString = $"{{ $and: [";
                addHeaderDone = true;
            }

            if (cqinfo.pageSize != 0)
            {
                filterString +=
                    AddFilterElementToString("pageSize", cqinfo.pageSize.ToString(), seperator, true, "$eq", false);
                seperator = ",";
            }

            if (addHeaderDone)
            {
                filterString += $"] }}";
            }

            return filterString;
        }

        public async Task<CommandQueueInformationDto> GetCommandQueueInformationAsync(CommandQueueInformationDto inCommandInfo)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            string filterString = GetCommandQueueInformationFilterString(inCommandInfo);

            do
            {
                try
                {
                    List<CommandQueueInformationDto> elementList = null;
                    elementList = webPageInformationCollection.Find<CommandQueueInformationDto>(filterString)
                        .ToList();

                    if (elementList.Count != 1)
                    {
                        for (int i = 1; i < elementList.Count; i++)
                        {
                            DeleteCommandQueueInformationById(elementList.ElementAt(i).ID);
                        }
                    }
                    return elementList.SingleOrDefault();
                }
                catch (FormatException fexp)
                {
                    Console.WriteLine($"Mongo Format exception {fexp.Message}");
                    return null;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    return null;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    return null;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
            } while (true);
        }

        public void ClearCommandQueueInformation()
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            var filterString = "{}";
            var result = webPageInformationCollection.DeleteMany(filterString);
        }

        public async Task<CommandQueueInformationDto> WriteCommandQueueInformationRecordAsync(CommandQueueInformationDto cqinfo)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            cqinfo.GuidString = cqinfo.GuidId.ToString();

            if (cqinfo.commandLine != null && cqinfo.commandLine.Length == 0)
            {
                cqinfo.commandLine = null;
            }

            var filterString = GetCommandQueueInformationFilterString(cqinfo);

            do
            {
                try
                {
                    if (cqinfo.lastUpdatedDateTime == DateTime.MinValue)
                    {
                        cqinfo.lastUpdatedDateTime = DateTime.Now;
                    }

                    List<CommandQueueInformationDto> elementList = null;
                    if (filterString != null)
                    {
                        elementList = webPageInformationCollection.Find<CommandQueueInformationDto>(filterString)
                            .ToList();
                    }

                    if (elementList == null || elementList.Count == 0)
                    {
                        cqinfo.lastUpdatedDateTime = DateTime.Now;
                        webPageInformationCollection.InsertOne(cqinfo);
                    }
                    else
                    {
                        cqinfo.lastUpdatedDateTime = DateTime.Now;
                        cqinfo.ID = elementList.SingleOrDefault().ID;
                        var result =
                            webPageInformationCollection.ReplaceOne(filterString, cqinfo,
                                new UpdateOptions() {IsUpsert = true});
                    }

                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (FormatException fexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {fexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
            } while (true);
        }

        public async Task<CommandFileLastIndexDto> GetCommandFileLastIndexRecordAsync(string filePath, string dayOfWeek)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandFileLastIndexDto>("commandfilelastindex");
            var filterString = $"{{ $and: [{{\"FilePath\":'{filePath.Replace("\\", "\\\\")}'}}";
            if (dayOfWeek != null)
            {
                filterString += $",{{\"DayOfWeek\":'{dayOfWeek}'}}";
            }
            else
            {
                filterString += $",{{\"DayOfWeek\": null}}";
            }

            filterString += @"]";
            filterString += @"}";

            do
            {
                try
                {
                    List<CommandFileLastIndexDto> elementList = null;
                    elementList = webPageInformationCollection.Find<CommandFileLastIndexDto>(filterString)
                        .ToList();

                    if (elementList.Count != 1)
                    {
                        for (int i = 1; i < elementList.Count; i++)
                        {
                            DeleteCommandQueueInformationById(elementList.ElementAt(i).ID);
                        }
                    }
                    return elementList.SingleOrDefault();
                }
                catch (FormatException fexp)
                {
                    Console.WriteLine($"Mongo Format exception {fexp.Message}");
                    return null;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    return null;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    return null;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.WriteLine($"We got an exception here {exp.Message}");
                    throw;
                }
            } while (true);
        }

        public async Task<CommandFileLastIndexDto> WriteCommandFileLastIndexRecordAsync(CommandFileLastIndexDto clinfo)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandFileLastIndexDto>("commandfilelastindex");

            var filePath = clinfo.FilePath;
            var dayOfWeek = clinfo.DayOfWeek;
            var filterString = $"{{ $and: [{{\"FilePath\":'{filePath.Replace("\\", "\\\\")}'}}";
            if (dayOfWeek != null)
            {
                filterString += $",{{\"DayOfWeek\":'{dayOfWeek}'}}";
            }
            else
            {
                filterString += $",{{\"DayOfWeek\": null}}";
            }
            filterString += @"]";
            filterString += @"}";

            do
            {
                try
                {

                    List<CommandFileLastIndexDto> elementList = null;
                    if (filterString != null)
                    {
                        elementList = webPageInformationCollection.Find<CommandFileLastIndexDto>(filterString)
                            .ToList();
                    }

                    if (elementList == null || elementList.Count == 0)
                    {
                        webPageInformationCollection.InsertOne(clinfo);
                    }
                    else
                    {
                        clinfo.ID = elementList.First().ID;
                        var result =
                            webPageInformationCollection.ReplaceOne(filterString, clinfo,
                                new UpdateOptions() { IsUpsert = true });
                    }

                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (FormatException fexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {fexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
            } while (true);
        }

        public long DeleteCommandQueueInformationById(ObjectId mongo)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            var filterString = $"{{_id: ObjectId('{mongo}')}}";
            var result = webPageInformationCollection.DeleteOne(filterString);
            return result.DeletedCount;
        }

        public long DeleteCommandQueueInformationByKey(string filterKeyword, string filterValue)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<UrlInformationDto>("commandQueue");
            var filterString = $"{{\"{filterKeyword}\": {{$eq: '{filterValue}'}}}}";
            var result = webPageInformationCollection.DeleteOne(filterString);
            return result.DeletedCount;
        }
        
        public async Task<CommandQueueInformationDto> GetCommandQueueInformationByGuidAsync(Guid guid)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            string filterString = "";
            if (guid != Guid.Empty)
            {
                filterString = $"{{\"GuidString\": '{guid.ToString()}' }}";
            }


            if (filterString.Length != 0)
            {
                return (await webPageInformationCollection.FindAsync(filterString)).SingleOrDefault();
            }

            return null;
        }

        public async Task<List<CommandQueueInformationDto>> GetAllCommandQueueInformatioAsync()
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<CommandQueueInformationDto>("commandQueue");
            string filterString = "";

            if (filterString.Length != 0)
            {
                return webPageInformationCollection.Find(filterString).ToList();
            }
            else
            {
                return webPageInformationCollection.Find("{}").ToList();
            }
        }
        public async Task<UrlInformationDto> GetUrlInformationAsync(string url, string photoId)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            string filterString = "";

            if (!string.IsNullOrEmpty(url))
            {

                filterString = $"{{\"AccessURL\": '{url}' }}";
            }
            else if (!string.IsNullOrEmpty(photoId))
            {
                filterString = $"{{\"PhotoID\": '{photoId}' }}";
            }

            if (filterString.Length != 0)
            {
                return webPageInformationCollection.Find(filterString).SingleOrDefault();
            }
            return null;
        }

        public async Task<List<UrlInformationDto>> GetUrlInformationAsync(int numberImagesViewMin, bool orderDesc,
            int rowLimit, string startLocation, DateTime? afterTime, Dictionary<string, string> searchTerms, bool unverifiedOnly, ObjectId? firstObjectId,
            string filterKeyword = null,
            string filterValue = null)
        {
            var filterString = "{}";

            if (filterString != null && filterKeyword != null)
            {
                filterString = "${{\"{filterKeyword}\": {{$eq: \"{filterValue}\"}}}}";
            }
            else
            {
                var searchTerm = searchTerms.SingleOrDefault();
                if (searchTerm.Key == "url")
                {
                    filterString = $"{{\"Host\": /{searchTerm.Value}/}}";
                }

                if (searchTerm.Key == "cmdfilename")
                {
                    filterString = $"{{\"SearchResultInfo.CmdFileName\": /{searchTerm.Value}/}}";
                }
            }

            var filterString2 = GetUrlInforfmationFilterString(startLocation, orderDesc, afterTime, searchTerms,
                filterKeyword, filterValue, unverifiedOnly);

            return ExecuteUrlQuery(numberImagesViewMin, orderDesc, filterString2);
        }

        public List<UrlInformationDto> ExecuteUrlQuery(int numberImagesViewMin, bool orderDesc, string filterString)
        {
            try
            {
                var webPageInformationCollection =
                    _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
                var resultsCursor = new List<UrlInformationDto>();
                if (orderDesc)
                {
                    resultsCursor = webPageInformationCollection.Find(filterString)
                        .Sort("{LastGatheredDateTime: -1}")
                        .Limit(numberImagesViewMin).ToList();
                }
                else
                {
                    resultsCursor = webPageInformationCollection.Find(filterString)
                        .Sort("{LastGatheredDateTime: 1}")
                        .Limit(numberImagesViewMin).ToList();
                }

                return resultsCursor;
            }
            catch (Exception exp)
            {
                Console.WriteLine($"We got an exception {exp.Message}");
                throw;
            }
        }

        public void ClearUrlInformation()
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            var filterString = "{}";
            var result = webPageInformationCollection.DeleteMany(filterString);
        }

        private static string _logServerLogCollection = "serverlogs";
        private static string _logApplicationLogCollection = "applicationlogs";


        public async Task<UrlInformationDto> WriteUrlInformationRecord(UrlInformationDto url, UrlInformationDto photoThere, List<GatheredFilesData> photoGathered)
        {
            var currentTime = DateTime.Now;
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            var filterString = $"{{\"AccessURL\": '{url.AccessURL.AbsoluteUri}' }}";
            var newImageWritten = false;
            var imageMovedToFrontOfQueue = false;

            do
            {
                try
                {
                    if (url.SearchResultInfo?.LastGatheredDateTime == DateTime.MinValue)
                    {
                        url.SearchResultInfo.LastGatheredDateTime = currentTime;
                    }

                    if (url.LastGatheredDateTime == null || url.LastGatheredDateTime == DateTime.MinValue)
                    {
                        url.LastGatheredDateTime = url.SearchResultInfo?.LastGatheredDateTime ?? currentTime;
                    }

                    if (photoThere == null)
                    {
                        if (photoGathered.Count == 0)
                        {
                            newImageWritten = true;
                            url.OrignalGatherDateTime = currentTime;
                            webPageInformationCollection.InsertOne(url);
                        }
                        else
                        {
                            newImageWritten = false;
                        }
                    }
                    else
                    {
                        imageMovedToFrontOfQueue = true;
                        url.ID = photoThere.ID;
                        url.OrignalGatherDateTime = photoThere.OrignalGatherDateTime ?? currentTime;
                        var result =
                            webPageInformationCollection.ReplaceOne(filterString, url,
                                new UpdateOptions() {IsUpsert = photoGathered.Count > 0});
                    }

                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    if (retItem != null)
                    {
                        if (retItem.LastGatheredDateTime != currentTime)
                        {
                            imageMovedToFrontOfQueue = true;
                            url.ID = retItem.ID;
                            url.OrignalGatherDateTime = retItem.OrignalGatherDateTime ?? currentTime;
                            url.LastGatheredDateTime = currentTime;
                            retItem.LastGatheredDateTime = currentTime;
                            var result =
                                webPageInformationCollection.ReplaceOne(filterString, url,
                                    new UpdateOptions() {IsUpsert = photoGathered.Count > 0});
                        }

                        retItem.NewImageWritten = newImageWritten;
                        retItem.ImageMovedToFrontOfQueue = imageMovedToFrontOfQueue;
                    }

                    return retItem;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.WriteLine($"Mongo exeption {exp.Message} for {filterString}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
            } while (true);
        }

        public long DeleteUrlInformationById(ObjectId mongo)
        {
            var webPageInformationCollection = _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            var filterString = $"{{_id: ObjectId('{mongo}')}}";
            var result = webPageInformationCollection.DeleteOne(filterString);
            return result.DeletedCount;
        }

        public long DeleteUrlInformationByKey(string filterKeyword, string filterValue)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            var filterString = $"{{\"{filterKeyword}\": {{$eq: '{filterValue}'}}}}";
            var result = webPageInformationCollection.DeleteOne(filterString);
            return result.DeletedCount;
        }

        public async Task<long> DeleteGatheredPhoto(GatheredFilesData gatheredFilesData)
        {
            var gatheredPhotosCollection =
                _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = GetGatheredPhotosFilterString(gatheredFilesData.PhotoUrl, gatheredFilesData.userName,
                gatheredFilesData.groupID,
                gatheredFilesData.albumID, gatheredFilesData.photoID);
            var result = gatheredPhotosCollection.DeleteMany(filterString);
            return result.DeletedCount;
        }

        public async Task<bool> WriteGatheredPhoto(GatheredFilesData gatheredFilesData)
        {
            var gatheredPhotosCollection =
                _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = GetGatheredPhotosFilterString(gatheredFilesData.PhotoUrl, gatheredFilesData.userName,
                gatheredFilesData.groupID,
                gatheredFilesData.albumID, gatheredFilesData.photoID);
            gatheredFilesData.ID = ObjectId.GenerateNewId(DateTime.Now);
            try {
                var elementList = gatheredPhotosCollection.Find<GatheredFilesData>(filterString).ToList();

                if (elementList == null || elementList.Count == 0)
                {
                    gatheredFilesData.TimeLastSeen = DateTime.Now;
                    gatheredPhotosCollection.InsertOne(gatheredFilesData);
                    return false;
                }
                else
                {
                    filterString = $"{{_id: ObjectId('{elementList.SingleOrDefault().ID}')}}";
                    gatheredFilesData.TimeLastSeen = DateTime.Now;
                    gatheredFilesData.ID = elementList.SingleOrDefault().ID;
                    var result =

                        gatheredPhotosCollection.ReplaceOne(filterString, gatheredFilesData, new UpdateOptions() { IsUpsert = true });
                    return true;
                }
            }
            catch (MongoDuplicateKeyException mexp)
            {
                Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                return false;
            }
            catch (MongoWriteException mwexp)
            {
                Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                return false;
            }
            catch (MongoCommandException mcexp)
            {
                Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                if (mcexp.Code == 8000)
                {
                    await CleanupAllInformationAsync();
                }

                throw;
            }
        }

        public async Task<List<GatheredFilesData>> GetPhotoGatheredInfoAsync(string url, string userName, string groupId, string albumId, string photoId)
        {
            var gatheredPhotosCollection =
                _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = GetGatheredPhotosFilterString(url, userName,
                groupId,
                albumId, photoId);
            var result = gatheredPhotosCollection.Find(filterString).ToList();
            return result;
        }

        public async Task<long> CleanupGatheredPhotosThatAreOlderThenAsync(TimeSpan numberOfDaysBack)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>("gatheredPhotos");
            var dateTimeString = DateTime.Now.Subtract(numberOfDaysBack).Date.ToString("yyyy-MM-dd");
            var filterString = $"{{ TimeLastSeen: {{$lt: ISODate('{dateTimeString}')}}}}";
            var result = searchInformationCollection.DeleteMany(filterString);
            return result.DeletedCount;
        }

        public async Task<long> CleanupDuplicatesGatheredPhotosByUrl()
        {
            var gatheredPhotosCollection =
                _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = "{}";
            var result = gatheredPhotosCollection.Find(filterString).ToList().GroupBy(t => t.PhotoUrl)
                .Where(a => a.Count() > 1);
            var retCount = result.Count();
            foreach (var entry in result)
            {
                for (int i = 1; i < entry.Count(); i++)
                {
                    var mongo = entry.ElementAt(i).ID;
                    filterString = $"{{_id: ObjectId('{mongo}')}}";
                    gatheredPhotosCollection.DeleteOne(filterString);
                }
            }
            return retCount;
        }

        public async Task<long> CleanupImageQueueThatAreOlderThenAsync(TimeSpan numberOfDaysBack)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>(MongoDbImageQueueCollection);
            var dateTimeString = DateTime.Now.Subtract(numberOfDaysBack).Date.ToString("yyyy-MM-dd");
            var filterString = $"{{ LastGatheredDateTime: {{$lt: ISODate('{dateTimeString}')}}}}";
            var result = searchInformationCollection.DeleteMany(filterString);
            return result.DeletedCount;
        }


        public async Task<List<GatheredFilesData>> GetGatheredPhotosInformation(string url, bool isFlickr,
            string userId, string groupId, string albumId, string photoId)
        {
            var gatheredPhotosCollection =
            _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = GetGatheredPhotosFilterString(url, userId, groupId, albumId, photoId);

            try
            {
                var elementList = gatheredPhotosCollection.Find<GatheredFilesData>(filterString);
                return elementList.ToList();
            }
            catch (Exception exp)
            {
                Console.WriteLine($"We got an exception {exp.Message}");
                throw;
            }
        }

        public long RemoveEntriesFromImageQueueInDisallowedList(List<DisallowedWordDto> list)
        {
            var retCount = 0l;
            var webPageInformationCollection = _lastGatherAppDatabase.GetCollection<UrlInformationDto>(MongoDbImageQueueCollection);
            foreach (var disallowedWord in list)
            {
                var keyword = disallowedWord.keyword;
                keyword = keyword.Replace("/", "\\/");
                var filterString = $"{{ AccessUrl:/{keyword}/}}";

                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ UserID:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ UserName:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ OwnerName:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ GroupName:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ PhotoID:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;

                filterString = $"{{ Tags:/{keyword}/}}";
                retCount += webPageInformationCollection.DeleteMany(filterString).DeletedCount;
            }
            return retCount;
        }

        private static string AddFilterElementToString(string key, string value, string seperator, bool exactMatch=true, string comparisonOperator=null, bool isDate=false)
        {
            if (value != null)
            {
                if (comparisonOperator != null)
                {
                    if (isDate)
                    {
                        return $"{seperator}{{ {key}: {{{comparisonOperator}: ISODate('{value}')}} }}";
                    }
                    else
                    {
                        value = value.Replace("\\", "\\\\");
                        value = value.Replace("'", "\\'");
                        
                        return $"{seperator}{{ {key}: {{{comparisonOperator}: '{value}'}} }}";

                    }
                }
                else
                {
                    if (exactMatch)
                    {
                        value = value.Replace("'", "\\'");

                        return $"{seperator}{{\"{key}\" : '{value}'}}";
                    }
                    else
                    {
                        return $"{seperator}{{\"{key}\" :/{value}/}}";
                    }
                }
            }
            return "";
        }

        private string GetUrlInforfmationFilterString(string startLocation, bool orderDesc, DateTime? afterTime,
            Dictionary<string, string> searchTerms, string filterKeyword = null,
            string filterValue = null, bool unverifiedOnly = false)
        {
            var filterString = "{}";
            bool addHeaderDone = false;
            var seperator = "";

            if (unverifiedOnly)
            {
                filterString = $"{{ $and: [";
                addHeaderDone = true;
                filterString += @"{VerifiedUrl: false}";
            }

            if (afterTime != null)
            {
                filterString = $"{{ $and: [";
                addHeaderDone = true;
                if (orderDesc)
                {
                    // 2019-10-07T07:08:23.841+00:00
                    filterString += AddFilterElementToString("LastGatheredDateTime",
                        afterTime.Value.ToString("yyyy-MM-ddTHH:mm:ss"), seperator, false, "$lte", true);
                }
                else
                {
                    filterString += AddFilterElementToString("LastGatheredDateTime",
                        afterTime.Value.ToString("yyyy-MM-ddTHH:mm:ss"), seperator, false, "$gte", true);
                }

                seperator = ",";
            }

            if (filterString != null && filterKeyword != null)
            {
                if (!addHeaderDone)
                {
                    filterString = $"{{ $and: [";
                    addHeaderDone = true;
                }

                filterString += AddFilterElementToString(filterKeyword, filterValue, seperator, false, "$eq", false);
                seperator = ",";
            }
            else
            {

                var searchTerm = searchTerms.SingleOrDefault();
                if (searchTerm.Key == "url")
                {
                    if (!addHeaderDone)
                    {
                        filterString = $"{{ $and: [";
                        addHeaderDone = true;
                    }

                    filterString += AddFilterElementToString("AccessURL", searchTerm.Value, seperator, false);
                    seperator = ",";
                }

                if (searchTerm.Key == "cmdfilename")
                {
                    if (!addHeaderDone)
                    {
                        filterString = $"{{ $and: [";
                        addHeaderDone = true;
                    }

                    filterString += AddFilterElementToString("SearchResultInfo.CmdFileName", searchTerm.Value,
                        seperator, false);
                    seperator = ",";
                }
            }

            if (addHeaderDone)
            {
                filterString += $"] }}";
            }

            return filterString;
        }

        private string GetGatheredPhotosFilterString(string url, string userId, string groupId, string albumId, string photoId)
        {
            var filterString = $"{{ $and: [";
            if (url != null)
            {
                filterString += AddFilterElementToString("PhotoUrl", url, "", true);
            }
            else if (photoId != null)
            {
                filterString += AddFilterElementToString("photoID", photoId, ",", true);
            }
            else if (userId != null)
            {
                filterString += AddFilterElementToString("userID", userId, ",", true);

            }
            else if (groupId != null)
            {
                filterString += AddFilterElementToString("groupID", groupId, ",", true);
            }
            else if (albumId != null)
            {
                filterString += AddFilterElementToString("albumID", albumId, ",", true);
            }

            filterString+= $"] }}";
            return filterString;
        }

        public async Task WriteLastSearchResultsInfo(SearchInformationResultsDto lastSearchResultInfo, bool replaceElement)
        {
            var searchInformationCollection =
                _lastGatherAppDatabase.GetCollection<SearchInformationResultsDto>("searchInformationResults");


            if (lastSearchResultInfo.SearchText == null)
            {
                lastSearchResultInfo.SearchText = "";
                Console.WriteLine("Found a null");
            }

            try
            {
                var elementList = await GetSearchInformationResultsAsync(lastSearchResultInfo.SearchText,
                    lastSearchResultInfo.PreviousDay, lastSearchResultInfo.PreviousWeek,
                    lastSearchResultInfo.SafeSearch, lastSearchResultInfo.SizeSearch);

                if (elementList != null && elementList.Count != 0)
                {
                    if (replaceElement)
                    {
                        var filterString = GetSearchInformationFilterString(lastSearchResultInfo.SearchText ?? "", lastSearchResultInfo.PreviousDay, lastSearchResultInfo.PreviousWeek, lastSearchResultInfo.SafeSearch, lastSearchResultInfo.SizeSearch);
                        var element = elementList.First();
                        lastSearchResultInfo.ID = element.ID;
                        var result =
                           searchInformationCollection.ReplaceOne(filterString, lastSearchResultInfo);
                        Console.WriteLine($"Result from mongo query was: {result.ToString()}");
                    }
                }
                else
                {
                    searchInformationCollection.InsertOne(lastSearchResultInfo);
                }
            }
            catch (MongoDuplicateKeyException mexp)
            {
                Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
            }
            catch (MongoWriteException mwexp)
            {
                Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
            }
            catch (MongoCommandException mcexp)
            {
                Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                if (mcexp.Code == 8000)
                {
                    await CleanupAllInformationAsync();
                }

                throw;
            }
        }

        public IEnumerable<DisallowedWordDto> GetDisallowedWords()
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<DisallowedWordDto>("disallowedWords");
            var filterString = $"{{}}";
            var elementList = webPageInformationCollection.Find<DisallowedWordDto>(filterString).ToList();
            return elementList;
        }

        public async Task<DisallowedWordDto> WriteDisallowedWordRecordAsync(DisallowedWordDto disallowedWord)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<DisallowedWordDto>("disallowedWords");
            var keyword = disallowedWord.keyword;
            keyword = keyword.Replace("/", "\\/");
            keyword = keyword.Replace("'", "\\'");
            if (keyword.Contains("'"))
            {
                Console.WriteLine("Had a special character");
            }
            var filterString = $"{{keyword: {{$eq: '{keyword}'}}}}";

            do
            {
                try
                {
                    var elementList = webPageInformationCollection.Find<DisallowedWordDto>(filterString).ToList();
                    if (elementList == null || elementList.Count == 0)
                    {
                        webPageInformationCollection.InsertOne(disallowedWord);
                    }
                    else
                    {
                        disallowedWord.ID = elementList.SingleOrDefault().ID;
                        var result =

                            webPageInformationCollection.ReplaceOne(filterString, disallowedWord,
                                new UpdateOptions() {IsUpsert = true});
                    }

                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoDuplicateKeyException mexp)
                {
                    Console.WriteLine($"Mongo duplicate key exception {mexp.ErrorMessage}");
                    var retItem1 = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem1;
                }
                catch (MongoWriteException mwexp)
                {
                    Console.WriteLine($"Mongo Write Exception {mwexp.WriteError} and {mwexp.Message}");
                    var retItem = webPageInformationCollection.Find(filterString).SingleOrDefault();
                    return retItem;
                }
                catch (MongoCommandException mcexp)
                {
                    Console.WriteLine($"mongo command exceptionL Code: {mcexp.Code} Message: {mcexp.ErrorMessage}");
                    if (mcexp.Code == 8000)
                    {
                        var count = await CleanupAllInformationAsync();
                        if (count == 0)
                        {
                            throw;
                        }
                    }
                }
            } while (true);
        }

        public long DeleteDisallowedKeyWord(DisallowedWordDto disallowedWord)
        {
            var webPageInformationCollection =
                _lastGatherAppDatabase.GetCollection<DisallowedWordDto>("disallowedWords");
            var filterString = $"{{keyword: {{$eq: '{disallowedWord.keyword}'}}}}";

            return webPageInformationCollection.DeleteMany(filterString).DeletedCount;

        }

        private async Task<long> MoveGatheredPhotosToNewStructureGatheredPhotosByUrl()
        {
            var gatheredPhotosCollection =
                _lastGatherAppDatabase.GetCollection<GatheredFilesData>("gatheredPhotos");
            var filterString = "{}";
            var result = gatheredPhotosCollection.Find(filterString).ToList().Where(t => t.PhotoUrl != null).ToList();

            foreach (var entry in result)
            {
                var mongo = entry.ID;
                entry.PhotoUrl = entry.url;
                entry.url = null;
                filterString = $"{{_id: ObjectId('{mongo}')}}";
                try
                {
                    gatheredPhotosCollection.ReplaceOne(filterString, entry);
                }
                catch (MongoWriteException mexp)
                {
                    Console.WriteLine($"We got an error converting {mexp.Message}");
                    gatheredPhotosCollection.DeleteOne(filterString);
                }
            }

            return result.Count();
        }
    }
}
