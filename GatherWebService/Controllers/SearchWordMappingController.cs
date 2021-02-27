using GatherCommon;
using GatherWebService.dtos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using RIZVIPSoftware.GatherDatabaseAccess;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace GatherWebService.Controllers
{
    /// <summary>
    /// Controller for Search Word Mapping
    /// </summary>
    public class SearchWordMappingController : ApiController
    {
        private static readonly object CmdLockEntry = new object();
        private static IGatherImageDatabase _dbAccessDisplay;
        private readonly GatherParameters _gparams = new GatherParameters();
        private readonly List<WordMappingDto> _currentList = new List<WordMappingDto>();

        private readonly string _fileName = Path.Combine(ConfigurationManager.AppSettings["DbFileRoot"], "searchWordMappings.json");

        /// <summary>
        /// Create a Search Word Mapping Controller
        /// </summary>
        public SearchWordMappingController()
        {
            FileStream rootFile = null;
            var serializer = new JsonSerializer();
            serializer.Converters.Add(new JavaScriptDateTimeConverter());
            serializer.NullValueHandling = NullValueHandling.Ignore;
            serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Converters.Add(new JavaScriptDateTimeConverter());
            jsonSettings.NullValueHandling = NullValueHandling.Ignore;

            _gparams.OutputPath = "bad";
            _gparams.TotalPerDirectory = 25;
            _gparams.LeaveImagesOnServer = false;
            _gparams.SiteDBFileRoot = ConfigurationManager.AppSettings["DbFileRoot"];

            FormEvents fest = new FormEvents();
            if (_dbAccessDisplay == null)
            {
                var gatherers = GetGathererForSite.GatherHandlers;
                var dsn = ConfigurationManager.AppSettings["DbDSN"];
                _dbAccessDisplay = new ImageDatabaseXML(_gparams, gatherers, dsn, fest, true);
            }

            if (_currentList.Count == 0 && File.Exists(_fileName))
            {
                try
                {
                    rootFile = File.Open(Path.Combine(_gparams.SiteDBFileRoot, _fileName), FileMode.Open, FileAccess.Read, FileShare.Read);
                    var sreader = new StreamReader(rootFile, true);
                    var jreader = new JsonTextReader(sreader);
                    _currentList = serializer.Deserialize<List<WordMappingDto>>(jreader);

                    jreader.Close();
                    rootFile.Close();
                }
                catch (IOException)
                {
                    rootFile?.Close();
                }
            }
            _gparams.OutputPath = "bad";
            _gparams.TotalPerDirectory = 25;
            _gparams.LeaveImagesOnServer = false;
            _gparams.SiteDBFileRoot = ConfigurationManager.AppSettings["DbFileRoot"];

            if (_dbAccessDisplay == null)
            {
                var gatherers = GetGathererForSite.GatherHandlers;
                var dsn = ConfigurationManager.AppSettings["DbDSN"];
                _dbAccessDisplay = new ImageDatabaseXML(_gparams, gatherers, dsn, fest, true);
            }

        }

        /// <summary>
        /// Return all the words in the WordMapping table
        /// </summary>
        /// <returns></returns>
        [HttpGet, ActionName("GetAll")]
        public IEnumerable<WordMappingDto> GetAll()
        {
            return _currentList;
        }

        /// <summary>
        /// Retrieve a specificy keyword mapping  or an empty DTO if there is none
        /// </summary>
        /// <param name="keyword"></param>
        /// <returns></returns>
        public WordMappingDto Get(string keyword)
        {
            try
            {
                return _currentList.Find(c => c.keyword.ToLower() == keyword.ToLower());
            }
            catch (Exception)
            {
                return new WordMappingDto();
            }
        }

        /// <summary>
        /// Called to add a Word Mapping to the system
        /// </summary>
        /// <param name="cqentry"></param>
        /// <returns></returns>
        [HttpPost, ActionName("add")]
        public HttpResponseMessage Add([FromBody]WordMappingDto cqentry)
        {
            var serializer = new JsonSerializer();
            serializer.Converters.Add(new JavaScriptDateTimeConverter());
            serializer.NullValueHandling = NullValueHandling.Ignore;
            serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Converters.Add(new JavaScriptDateTimeConverter());
            jsonSettings.NullValueHandling = NullValueHandling.Ignore;
            cqentry.keyword = cqentry.keyword.ToLower();
            var entryFoundCheck = Get(cqentry.keyword);
            if (entryFoundCheck != null)
            {
                lock (CmdLockEntry)
                {
                    _currentList.Remove(entryFoundCheck);
                    _currentList.Add(cqentry);
                    var swriter = new StreamWriter(_fileName, false);
                    using (JsonWriter jwriter = new JsonTextWriter(swriter))
                    {
                        serializer.Serialize(jwriter, _currentList);
                        swriter.WriteLine();
                    }
                }
            }
            else
            {
                lock (CmdLockEntry)
                {
                        _currentList.Add(cqentry);
                        var swriter = new StreamWriter(_fileName, false);
                        using (var jwriter = new JsonTextWriter(swriter))
                        {
                            serializer.Serialize(jwriter, _currentList);
                            swriter.WriteLine();
                        }
                }
            }

            var rspMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    $"The mapping word {cqentry.keyword} was successfully added. Total Entries now {_currentList.Count}")
            };
            return rspMessage;
        }

        /// <summary>
        /// Service call to Delete a word mapping
        /// </summary>
        /// <param name="keyword">Word to be deleted</param>
        /// <returns></returns>
        public HttpResponseMessage Delete(string keyword)
        {
            var serializer = new JsonSerializer();
            serializer.Converters.Add(new JavaScriptDateTimeConverter());
            serializer.NullValueHandling = NullValueHandling.Ignore;
            serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Converters.Add(new JavaScriptDateTimeConverter());
            jsonSettings.NullValueHandling = NullValueHandling.Ignore;

            lock (CmdLockEntry)
            {
                var cqentry = Get(keyword);
                if (cqentry != null)
                {
                    _currentList.Remove(cqentry);
                    using (var swriter = new StreamWriter(_fileName, false))
                    {
                        using (var jwriter = new JsonTextWriter(swriter))
                        {
                            serializer.Serialize(jwriter, _currentList);
                            swriter.WriteLine();
                        }
                    }
                }
            }

            var rspMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent($"The mapping word {keyword} was successfully deleted. Total Entries now {_currentList.Count}")
            };
            return rspMessage;

        }
    }
}
