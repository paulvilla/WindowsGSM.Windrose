using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WindowsGSM.Functions;
using WindowsGSM.GameServer.Engine;

namespace WindowsGSM.Plugins
{
    public class Windrose : SteamCMDAgent
    {
        private const string GeneratedByServerMap = "GeneratedByServer";
        private const string DefaultWorldName = "My Windrose World";
        private const string SharedQuestsParameterKey = "{\"TagName\": \"WDS.Parameter.Coop.SharedQuests\"}";
        private const string EasyExploreParameterKey = "{\"TagName\": \"WDS.Parameter.EasyExplore\"}";
        private const string MobHealthMultiplierParameterKey = "{\"TagName\": \"WDS.Parameter.MobHealthMultiplier\"}";
        private const string MobDamageMultiplierParameterKey = "{\"TagName\": \"WDS.Parameter.MobDamageMultiplier\"}";
        private const string ShipsHealthMultiplierParameterKey = "{\"TagName\": \"WDS.Parameter.ShipsHealthMultiplier\"}";
        private const string ShipsDamageMultiplierParameterKey = "{\"TagName\": \"WDS.Parameter.ShipsDamageMultiplier\"}";
        private const string BoardingDifficultyMultiplierParameterKey = "{\"TagName\": \"WDS.Parameter.BoardingDifficultyMultiplier\"}";
        private const string StatsCorrectionModifierParameterKey = "{\"TagName\": \"WDS.Parameter.Coop.StatsCorrectionModifier\"}";
        private const string ShipStatsCorrectionModifierParameterKey = "{\"TagName\": \"WDS.Parameter.Coop.ShipStatsCorrectionModifier\"}";
        private const string CombatDifficultyParameterKey = "{\"TagName\": \"WDS.Parameter.CombatDifficulty\"}";
        private const string CombatDifficultyTagPrefix = "WDS.Parameter.CombatDifficulty.";

        public Plugin Plugin = new Plugin
        {
            name = "WindowsGSM.Windrose",
            author = "paulvilla",
            description = "WindowsGSM plugin for the Windrose dedicated server.",
            version = "1.1.0",
            url = "https://github.com/paulvilla/WindowsGSM.Windrose",
            color = "#005386"
        };

        private readonly ServerConfig _serverData;

        public Windrose(ServerConfig serverData) : base(serverData) => base.serverData = _serverData = serverData;

        public override bool loginAnonymous { get; set; } = true;
        public override string AppId { get; set; } = "4129620";
        public override string StartPath { get; set; } = @"R5\Binaries\Win64\WindroseServer-Win64-Shipping.exe";

        public string FullName = "Windrose Dedicated Server";
        public bool AllowsEmbedConsole = true;
        public int PortIncrements = 2;
        public object QueryMethod = null;

        public string Port = "7777";
        public string QueryPort = "7778";
        public string Defaultmap = DefaultWorldName;
        public string Maxplayers = "10";
        public string Additional = string.Empty;

        public async void CreateServerCFG()
        {
            Error = null;
            await Task.Run(() =>
            {
                EnsureDefaultServerMap();
                TrySyncWindowsGsmConfigFromWorldDescription();
                WindroseParameterSettings settings = ParsePluginParameters(_serverData.ServerParam);
                PersistPluginParameters(settings);
                UpsertServerDescription(settings);
                UpsertWorldDescription(settings);
            });
        }

        public Task<Process> Start()
        {
            Error = null;
            Notice = null;
            string serverRoot = ServerPath.GetServersServerFiles(_serverData.ServerID);
            string shipExePath = ServerPath.GetServersServerFiles(_serverData.ServerID, StartPath);
            string serverDescriptionPath = GetServerDescriptionPath();
            string previousInviteCode = TryReadInviteCode(serverDescriptionPath);
            SynchronizationContext consoleContext = SynchronizationContext.Current;
            DateTime launchTime = DateTime.Now;
            BufferedConsoleMirror consoleMirror = null;
            if (!File.Exists(shipExePath))
            {
                Error = $"{Path.GetFileName(shipExePath)} not found ({shipExePath})";
                return Task.FromResult<Process>(null);
            }

            TrySyncWindowsGsmConfigFromWorldDescription();
            WindroseParameterSettings settings = ParsePluginParameters(_serverData.ServerParam);
            PersistPluginParameters(settings);
            UpsertServerDescription(settings);
            UpsertWorldDescription(settings);
            var param = new StringBuilder(BuildBaseLaunchArguments(AllowsEmbedConsole));
            if (!string.IsNullOrWhiteSpace(_serverData.ServerIP) && _serverData.ServerIP != "0.0.0.0")
            {
                param.Append($" -MULTIHOME={_serverData.ServerIP}");
            }

            if (!string.IsNullOrWhiteSpace(_serverData.ServerPort))
            {
                param.Append($" -PORT={_serverData.ServerPort}");
            }

            if (!string.IsNullOrWhiteSpace(_serverData.ServerQueryPort))
            {
                param.Append($" -QUERYPORT={_serverData.ServerQueryPort}");
            }

            if (!string.IsNullOrWhiteSpace(settings.LaunchArguments))
            {
                param.Append(' ');
                param.Append(settings.LaunchArguments);
            }

            Process process;
            if (!AllowsEmbedConsole)
            {
                process = new Process
                {
                    StartInfo =
                    {
                        WorkingDirectory = serverRoot,
                        FileName = shipExePath,
                        Arguments = param.ToString(),
                        WindowStyle = ProcessWindowStyle.Hidden,
                        CreateNoWindow = true,
                        UseShellExecute = false
                    },
                    EnableRaisingEvents = true
                };
            }
            else
            {
                process = new Process
                {
                    StartInfo =
                    {
                        WorkingDirectory = serverRoot,
                        FileName = shipExePath,
                        Arguments = param.ToString(),
                        WindowStyle = ProcessWindowStyle.Hidden,
                        CreateNoWindow = true,
                        UseShellExecute = false,
                        RedirectStandardInput = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true
                    },
                    EnableRaisingEvents = true
                };

                consoleMirror = new BufferedConsoleMirror(_serverData.ServerID, consoleContext);
                process.OutputDataReceived += consoleMirror.AddOutput;
                process.ErrorDataReceived += consoleMirror.AddOutput;
                process.Exited += (_, __) => consoleMirror.Dispose();
            }

            try
            {
                process.Start();
                if (process.StartInfo.RedirectStandardOutput)
                {
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();
                }

                if (WaitForEarlyExit(process, 2500))
                {
                    consoleMirror?.FlushNow();
                    consoleMirror?.Dispose();
                    Error = BuildStartupFailureMessage(process, shipExePath, launchTime);
                    return Task.FromResult<Process>(null);
                }

                _ = HideProcessWindowsAsync(process.Id);
                _ = PublishInviteCodeAsync(process, serverDescriptionPath, previousInviteCode, consoleMirror, consoleContext);
                _ = SyncWindowsGsmConfigFromWorldDescriptionAsync(process);

                string worldExample = ServerPath.GetServersServerFiles(_serverData.ServerID, @"R5\Saved\SaveProfiles");
                if (!Directory.Exists(worldExample))
                {
                    Notice = "WorldDescription.json is generated by Windrose after the first successful start. Supported world settings are then mirrored into Server Start Param in Edit Config.";
                }

                return Task.FromResult(process);
            }
            catch (Exception e)
            {
                consoleMirror?.Dispose();
                Error = e.Message;
                return Task.FromResult<Process>(null);
            }
        }

        public async Task Stop(Process p)
        {
            if (p == null)
            {
                return;
            }

            try
            {
                if (p.HasExited)
                {
                    return;
                }
            }
            catch
            {
                return;
            }

            bool sentSave = SendConsoleCommand(p, "save world");
            if (sentSave)
            {
                await Task.Delay(3000);
            }

            bool sentQuit = SendConsoleCommand(p, "quit");
            if (sentQuit && await WaitForExitAsync(p, 20000))
            {
                return;
            }

            try
            {
                if (!p.HasExited && p.MainWindowHandle != IntPtr.Zero)
                {
                    p.CloseMainWindow();
                    if (await WaitForExitAsync(p, 5000))
                    {
                        return;
                    }
                }
            }
            catch
            {
            }

            try
            {
                if (!p.HasExited)
                {
                    p.Kill();
                }
            }
            catch
            {
            }
        }

        private void UpsertServerDescription(WindroseParameterSettings settings)
        {
            try
            {
                Error = null;
                string configPath = GetServerDescriptionPath();
                Directory.CreateDirectory(Path.GetDirectoryName(configPath));

                JObject root = new JObject();
                if (File.Exists(configPath))
                {
                    try
                    {
                        root = JObject.Parse(File.ReadAllText(configPath));
                    }
                    catch
                    {
                        root = new JObject();
                    }
                }

                settings ??= ParsePluginParameters(_serverData.ServerParam);
                JObject persistent = root["ServerDescription_Persistent"] as JObject ?? new JObject();

                string existingPassword = persistent.Value<string>("Password") ?? string.Empty;
                bool existingProtected = persistent.Value<bool?>("IsPasswordProtected") ?? false;

                string password = settings.Password ?? existingPassword;
                bool isPasswordProtected = settings.IsPasswordProtected
                    ?? (settings.Password != null ? !string.IsNullOrWhiteSpace(settings.Password) : existingProtected);

                if (!isPasswordProtected)
                {
                    password = string.Empty;
                }

                string proxyAddress = !string.IsNullOrWhiteSpace(settings.P2pProxyAddress)
                    ? settings.P2pProxyAddress
                    : (persistent.Value<string>("P2pProxyAddress") ?? GetDefaultProxyAddress());

                var output = new JObject
                {
                    ["Version"] = root["Version"] ?? 1,
                    ["DeploymentId"] = root["DeploymentId"] ?? string.Empty,
                    ["ServerDescription_Persistent"] = new JObject
                    {
                        ["PersistentServerId"] = persistent["PersistentServerId"] ?? string.Empty,
                        ["InviteCode"] = persistent["InviteCode"] ?? string.Empty,
                        ["IsPasswordProtected"] = isPasswordProtected,
                        ["Password"] = password,
                        ["ServerName"] = string.IsNullOrWhiteSpace(_serverData.ServerName) ? "Windrose Dedicated Server" : _serverData.ServerName,
                        ["WorldIslandId"] = persistent["WorldIslandId"] ?? string.Empty,
                        ["MaxPlayerCount"] = ParseIntOrDefault(_serverData.ServerMaxPlayer, 10),
                        ["P2pProxyAddress"] = proxyAddress
                    }
                };

                File.WriteAllText(configPath, output.ToString(Formatting.Indented), new UTF8Encoding(false));
            }
            catch (Exception e)
            {
                Error = e.Message;
            }
        }

        private void UpsertWorldDescription(WindroseParameterSettings settings)
        {
            string configPath = TryGetWorldDescriptionPath();
            if (string.IsNullOrWhiteSpace(configPath) || !File.Exists(configPath))
            {
                return;
            }

            try
            {
                settings ??= ParsePluginParameters(_serverData.ServerParam);

                JObject root = JObject.Parse(File.ReadAllText(configPath));
                JObject worldDescription = root["WorldDescription"] as JObject;
                if (worldDescription == null)
                {
                    return;
                }

                JObject worldSettings = worldDescription["WorldSettings"] as JObject ?? new JObject();
                JObject boolParameters = worldSettings["BoolParameters"] as JObject ?? new JObject();
                JObject floatParameters = worldSettings["FloatParameters"] as JObject ?? new JObject();
                JObject tagParameters = worldSettings["TagParameters"] as JObject ?? new JObject();

                string configuredWorldName = ResolveConfiguredWorldName(settings, worldDescription.Value<string>("WorldName"));
                if (!string.IsNullOrWhiteSpace(configuredWorldName))
                {
                    worldDescription["WorldName"] = configuredWorldName;
                }

                string configuredWorldPresetType = ResolveConfiguredWorldPresetType(settings, worldDescription.Value<string>("WorldPresetType"));
                if (!string.IsNullOrWhiteSpace(configuredWorldPresetType))
                {
                    worldDescription["WorldPresetType"] = configuredWorldPresetType;
                }

                ApplyNullableBoolean(boolParameters, SharedQuestsParameterKey, settings.SharedQuests);
                ApplyNullableBoolean(boolParameters, EasyExploreParameterKey, settings.EasyExplore);
                ApplyNullableDouble(floatParameters, MobHealthMultiplierParameterKey, settings.MobHealthMultiplier);
                ApplyNullableDouble(floatParameters, MobDamageMultiplierParameterKey, settings.MobDamageMultiplier);
                ApplyNullableDouble(floatParameters, ShipsHealthMultiplierParameterKey, settings.ShipsHealthMultiplier);
                ApplyNullableDouble(floatParameters, ShipsDamageMultiplierParameterKey, settings.ShipsDamageMultiplier);
                ApplyNullableDouble(floatParameters, BoardingDifficultyMultiplierParameterKey, settings.BoardingDifficultyMultiplier);
                ApplyNullableDouble(floatParameters, StatsCorrectionModifierParameterKey, settings.StatsCorrectionModifier);
                ApplyNullableDouble(floatParameters, ShipStatsCorrectionModifierParameterKey, settings.ShipStatsCorrectionModifier);

                string combatDifficultyTag = BuildCombatDifficultyTag(settings.CombatDifficulty);
                if (!string.IsNullOrWhiteSpace(combatDifficultyTag))
                {
                    tagParameters[CombatDifficultyParameterKey] = new JObject
                    {
                        ["TagName"] = combatDifficultyTag
                    };
                }

                worldSettings["BoolParameters"] = boolParameters;
                worldSettings["FloatParameters"] = floatParameters;
                worldSettings["TagParameters"] = tagParameters;
                worldDescription["WorldSettings"] = worldSettings;
                root["WorldDescription"] = worldDescription;

                File.WriteAllText(configPath, root.ToString(Formatting.Indented), new UTF8Encoding(false));
            }
            catch (Exception e)
            {
                Error = e.Message;
            }
        }

        private string GetServerDescriptionPath()
        {
            return ServerPath.GetServersServerFiles(_serverData.ServerID, @"R5\ServerDescription.json");
        }

        private string TryGetWorldDescriptionPath()
        {
            string saveProfilesRoot = ServerPath.GetServersServerFiles(_serverData.ServerID, @"R5\Saved\SaveProfiles");
            if (!Directory.Exists(saveProfilesRoot))
            {
                return null;
            }

            string[] configPaths;
            try
            {
                configPaths = Directory.GetFiles(saveProfilesRoot, "WorldDescription.json", SearchOption.AllDirectories);
            }
            catch
            {
                return null;
            }

            if (configPaths.Length == 0)
            {
                return null;
            }

            string worldIslandId = TryReadWorldIslandId(GetServerDescriptionPath());
            string mostRecentPath = null;
            DateTime mostRecentWriteTime = DateTime.MinValue;

            foreach (string configPath in configPaths)
            {
                if (!string.IsNullOrWhiteSpace(worldIslandId) && configPath.IndexOf(worldIslandId, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return configPath;
                }

                try
                {
                    DateTime writeTime = File.GetLastWriteTimeUtc(configPath);
                    if (writeTime > mostRecentWriteTime)
                    {
                        mostRecentWriteTime = writeTime;
                        mostRecentPath = configPath;
                    }
                }
                catch
                {
                }
            }

            return mostRecentPath;
        }

        private bool TrySyncWindowsGsmConfigFromWorldDescription()
        {
            WorldDescriptionSnapshot snapshot = TryReadWorldDescriptionSnapshot(TryGetWorldDescriptionPath());
            if (snapshot == null)
            {
                return false;
            }

            WindroseParameterSettings settings = ParsePluginParameters(_serverData.ServerParam);
            bool changed = false;

            if (string.IsNullOrWhiteSpace(settings.WorldPresetType) && !string.IsNullOrWhiteSpace(snapshot.WorldPresetType))
            {
                settings.WorldPresetType = snapshot.WorldPresetType;
                changed = true;
            }

            if (!settings.SharedQuests.HasValue && snapshot.SharedQuests.HasValue)
            {
                settings.SharedQuests = snapshot.SharedQuests;
                changed = true;
            }

            if (!settings.EasyExplore.HasValue && snapshot.EasyExplore.HasValue)
            {
                settings.EasyExplore = snapshot.EasyExplore;
                changed = true;
            }

            if (!settings.MobHealthMultiplier.HasValue && snapshot.MobHealthMultiplier.HasValue)
            {
                settings.MobHealthMultiplier = snapshot.MobHealthMultiplier;
                changed = true;
            }

            if (!settings.MobDamageMultiplier.HasValue && snapshot.MobDamageMultiplier.HasValue)
            {
                settings.MobDamageMultiplier = snapshot.MobDamageMultiplier;
                changed = true;
            }

            if (!settings.ShipsHealthMultiplier.HasValue && snapshot.ShipsHealthMultiplier.HasValue)
            {
                settings.ShipsHealthMultiplier = snapshot.ShipsHealthMultiplier;
                changed = true;
            }

            if (!settings.ShipsDamageMultiplier.HasValue && snapshot.ShipsDamageMultiplier.HasValue)
            {
                settings.ShipsDamageMultiplier = snapshot.ShipsDamageMultiplier;
                changed = true;
            }

            if (!settings.BoardingDifficultyMultiplier.HasValue && snapshot.BoardingDifficultyMultiplier.HasValue)
            {
                settings.BoardingDifficultyMultiplier = snapshot.BoardingDifficultyMultiplier;
                changed = true;
            }

            if (!settings.StatsCorrectionModifier.HasValue && snapshot.StatsCorrectionModifier.HasValue)
            {
                settings.StatsCorrectionModifier = snapshot.StatsCorrectionModifier;
                changed = true;
            }

            if (!settings.ShipStatsCorrectionModifier.HasValue && snapshot.ShipStatsCorrectionModifier.HasValue)
            {
                settings.ShipStatsCorrectionModifier = snapshot.ShipStatsCorrectionModifier;
                changed = true;
            }

            if (string.IsNullOrWhiteSpace(settings.CombatDifficulty) && !string.IsNullOrWhiteSpace(snapshot.CombatDifficulty))
            {
                settings.CombatDifficulty = snapshot.CombatDifficulty;
                changed = true;
            }

            if (PersistPluginParameters(settings))
            {
                changed = true;
            }

            string resolvedServerMap = ResolveDisplayedServerMap(snapshot);
            string currentServerMap = (_serverData.ServerMap ?? string.Empty).Trim();
            bool shouldUpdateServerMap = !string.IsNullOrWhiteSpace(resolvedServerMap)
                && (!IsMeaningfulServerMap(currentServerMap)
                    || (!string.IsNullOrWhiteSpace(snapshot.WorldPresetType)
                        && string.Equals(currentServerMap, snapshot.WorldPresetType, StringComparison.Ordinal)));
            if (shouldUpdateServerMap && !string.Equals(currentServerMap, resolvedServerMap, StringComparison.Ordinal))
            {
                _serverData.ServerMap = resolvedServerMap;
                ServerConfig.SetSetting(_serverData.ServerID, ServerConfig.SettingName.ServerMap, resolvedServerMap);
                changed = true;
            }

            return changed;
        }

        private async Task SyncWindowsGsmConfigFromWorldDescriptionAsync(Process process)
        {
            for (int attempt = 0; attempt < 30; attempt++)
            {
                try
                {
                    if (process == null || process.HasExited)
                    {
                        return;
                    }
                }
                catch
                {
                    return;
                }

                if (TrySyncWindowsGsmConfigFromWorldDescription())
                {
                    return;
                }

                await Task.Delay(1000);
            }
        }

        private async Task PublishInviteCodeAsync(Process process, string configPath, string previousInviteCode, BufferedConsoleMirror consoleMirror, SynchronizationContext consoleContext)
        {
            string lastReportedInviteCode = previousInviteCode;

            if (!string.IsNullOrWhiteSpace(previousInviteCode))
            {
                AddServerConsoleLine(consoleMirror, consoleContext, $"[Windrose] InviteCode: {previousInviteCode}");
            }

            for (int attempt = 0; attempt < 30; attempt++)
            {
                try
                {
                    if (process == null || process.HasExited)
                    {
                        return;
                    }
                }
                catch
                {
                    return;
                }

                string inviteCode = TryReadInviteCode(configPath);
                if (!string.IsNullOrWhiteSpace(inviteCode) && !string.Equals(inviteCode, lastReportedInviteCode, StringComparison.Ordinal))
                {
                    AddServerConsoleLine(consoleMirror, consoleContext, $"[Windrose] InviteCode: {inviteCode}");
                    return;
                }

                await Task.Delay(1000);
            }
        }

        private void AddServerConsoleLine(BufferedConsoleMirror consoleMirror, SynchronizationContext consoleContext, string text)
        {
            if (consoleMirror != null)
            {
                consoleMirror.AddLine(text);
                consoleMirror.FlushNow();
                return;
            }

            void AppendToConsole(object _)
            {
                if (!int.TryParse(_serverData.ServerID, out int serverId))
                {
                    return;
                }

                if (MainWindow._serverMetadata.TryGetValue(serverId, out MainWindow.ServerMetadata metadata) && metadata?.ServerConsole != null)
                {
                    metadata.ServerConsole.Add(text);
                }
            }

            try
            {
                if (consoleContext == null || consoleContext == SynchronizationContext.Current)
                {
                    AppendToConsole(null);
                    return;
                }

                consoleContext.Post(AppendToConsole, null);
            }
            catch
            {
            }
        }

        private static string TryReadInviteCode(string configPath)
        {
            if (!File.Exists(configPath))
            {
                return string.Empty;
            }

            try
            {
                JObject root = JObject.Parse(File.ReadAllText(configPath));
                return root["ServerDescription_Persistent"]?.Value<string>("InviteCode") ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        private static string TryReadWorldIslandId(string configPath)
        {
            if (!File.Exists(configPath))
            {
                return string.Empty;
            }

            try
            {
                JObject root = JObject.Parse(File.ReadAllText(configPath));
                return root["ServerDescription_Persistent"]?.Value<string>("WorldIslandId") ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        private string GetDefaultProxyAddress()
        {
            if (!string.IsNullOrWhiteSpace(_serverData.ServerIP) && _serverData.ServerIP != "0.0.0.0")
            {
                return _serverData.ServerIP;
            }

            return "127.0.0.1";
        }

        private static int ParseIntOrDefault(string value, int fallback)
        {
            return int.TryParse(value, out int parsed) ? parsed : fallback;
        }

        private static bool IsMeaningfulServerMap(string value)
        {
            return !string.IsNullOrWhiteSpace(value) && !string.Equals(value, GeneratedByServerMap, StringComparison.OrdinalIgnoreCase);
        }

        private void EnsureDefaultServerMap()
        {
            string currentServerMap = (_serverData.ServerMap ?? string.Empty).Trim();
            if (IsMeaningfulServerMap(currentServerMap))
            {
                return;
            }

            _serverData.ServerMap = DefaultWorldName;
            ServerConfig.SetSetting(_serverData.ServerID, ServerConfig.SettingName.ServerMap, DefaultWorldName);
        }

        private bool PersistPluginParameters(WindroseParameterSettings settings)
        {
            string rebuiltServerParam = BuildPluginParameters(settings);
            string currentServerParam = (_serverData.ServerParam ?? string.Empty).Trim();
            if (string.Equals(rebuiltServerParam, currentServerParam, StringComparison.Ordinal))
            {
                return false;
            }

            _serverData.ServerParam = rebuiltServerParam;
            ServerConfig.SetSetting(_serverData.ServerID, ServerConfig.SettingName.ServerParam, rebuiltServerParam);
            return true;
        }

        private string ResolveConfiguredWorldName(WindroseParameterSettings settings, string existingWorldName)
        {
            if (IsMeaningfulServerMap(_serverData.ServerMap))
            {
                return _serverData.ServerMap.Trim();
            }

            return ResolveWorldName(existingWorldName);
        }

        private static string ResolveDisplayedServerMap(WorldDescriptionSnapshot snapshot)
        {
            return ResolveWorldName(snapshot?.WorldName);
        }

        private static string ResolveWorldName(string value)
        {
            return string.IsNullOrWhiteSpace(value) ? DefaultWorldName : value.Trim();
        }

        private static void ApplyNullableBoolean(JObject target, string key, bool? value)
        {
            if (target != null && value.HasValue)
            {
                target[key] = value.Value;
            }
        }

        private static void ApplyNullableDouble(JObject target, string key, double? value)
        {
            if (target != null && value.HasValue)
            {
                target[key] = value.Value;
            }
        }

        private string ResolveConfiguredWorldPresetType(WindroseParameterSettings settings, string existingWorldPresetType)
        {
            if (!string.IsNullOrWhiteSpace(settings?.WorldPresetType))
            {
                return settings.WorldPresetType;
            }

            return existingWorldPresetType;
        }

        private static string BuildCombatDifficultyTag(string value)
        {
            string normalizedValue = NormalizeCombatDifficulty(value);
            return string.IsNullOrWhiteSpace(normalizedValue) ? null : CombatDifficultyTagPrefix + normalizedValue;
        }

        private static string NormalizeCombatDifficulty(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            string normalizedValue = value.Trim();
            if (normalizedValue.StartsWith(CombatDifficultyTagPrefix, StringComparison.OrdinalIgnoreCase))
            {
                normalizedValue = normalizedValue.Substring(CombatDifficultyTagPrefix.Length);
            }

            int lastDotIndex = normalizedValue.LastIndexOf('.');
            if (lastDotIndex >= 0 && lastDotIndex < normalizedValue.Length - 1)
            {
                normalizedValue = normalizedValue.Substring(lastDotIndex + 1);
            }

            return string.IsNullOrWhiteSpace(normalizedValue) ? null : normalizedValue;
        }

        private static bool? ParseNullableBoolean(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            if (bool.TryParse(value, out bool parsedBoolean))
            {
                return parsedBoolean;
            }

            if (value == "1")
            {
                return true;
            }

            if (value == "0")
            {
                return false;
            }

            return null;
        }

        private static double? ParseNullableDouble(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsedDouble))
            {
                return parsedDouble;
            }

            if (double.TryParse(value, out parsedDouble))
            {
                return parsedDouble;
            }

            return null;
        }

        private static double? ParseNullableDouble(JToken token)
        {
            if (token == null)
            {
                return null;
            }

            switch (token.Type)
            {
                case JTokenType.Float:
                case JTokenType.Integer:
                    return token.Value<double>();
                case JTokenType.String:
                    return ParseNullableDouble(token.Value<string>());
                default:
                    return null;
            }
        }

        private WorldDescriptionSnapshot TryReadWorldDescriptionSnapshot(string configPath)
        {
            if (string.IsNullOrWhiteSpace(configPath) || !File.Exists(configPath))
            {
                return null;
            }

            try
            {
                JObject root = JObject.Parse(File.ReadAllText(configPath));
                JObject worldDescription = root["WorldDescription"] as JObject;
                if (worldDescription == null)
                {
                    return null;
                }

                JObject worldSettings = worldDescription["WorldSettings"] as JObject ?? new JObject();
                JObject boolParameters = worldSettings["BoolParameters"] as JObject ?? new JObject();
                JObject floatParameters = worldSettings["FloatParameters"] as JObject ?? new JObject();
                JObject tagParameters = worldSettings["TagParameters"] as JObject ?? new JObject();

                return new WorldDescriptionSnapshot
                {
                    WorldName = worldDescription.Value<string>("WorldName") ?? string.Empty,
                    WorldPresetType = worldDescription.Value<string>("WorldPresetType") ?? string.Empty,
                    SharedQuests = boolParameters.Value<bool?>(SharedQuestsParameterKey),
                    EasyExplore = boolParameters.Value<bool?>(EasyExploreParameterKey),
                    MobHealthMultiplier = ParseNullableDouble(floatParameters[MobHealthMultiplierParameterKey]),
                    MobDamageMultiplier = ParseNullableDouble(floatParameters[MobDamageMultiplierParameterKey]),
                    ShipsHealthMultiplier = ParseNullableDouble(floatParameters[ShipsHealthMultiplierParameterKey]),
                    ShipsDamageMultiplier = ParseNullableDouble(floatParameters[ShipsDamageMultiplierParameterKey]),
                    BoardingDifficultyMultiplier = ParseNullableDouble(floatParameters[BoardingDifficultyMultiplierParameterKey]),
                    StatsCorrectionModifier = ParseNullableDouble(floatParameters[StatsCorrectionModifierParameterKey]),
                    ShipStatsCorrectionModifier = ParseNullableDouble(floatParameters[ShipStatsCorrectionModifierParameterKey]),
                    CombatDifficulty = NormalizeCombatDifficulty(tagParameters[CombatDifficultyParameterKey]?["TagName"]?.Value<string>())
                };
            }
            catch
            {
                return null;
            }
        }

        private static string BuildPluginParameters(WindroseParameterSettings settings)
        {
            var parameters = new List<string>();

            AppendNamedParameter(parameters, "Password", settings.Password, true);
            if (settings.IsPasswordProtected.HasValue)
            {
                AppendNamedParameter(parameters, "IsPasswordProtected", settings.IsPasswordProtected.Value ? "true" : "false", false);
            }

            AppendNamedParameter(parameters, "P2pProxyAddress", settings.P2pProxyAddress, false);
            AppendNamedParameter(parameters, "WorldPresetType", settings.WorldPresetType, false);

            if (settings.SharedQuests.HasValue)
            {
                AppendNamedParameter(parameters, "SharedQuests", settings.SharedQuests.Value ? "true" : "false", false);
            }

            if (settings.EasyExplore.HasValue)
            {
                AppendNamedParameter(parameters, "EasyExplore", settings.EasyExplore.Value ? "true" : "false", false);
            }

            AppendNamedParameter(parameters, "MobHealthMultiplier", FormatNullableDouble(settings.MobHealthMultiplier), false);
            AppendNamedParameter(parameters, "MobDamageMultiplier", FormatNullableDouble(settings.MobDamageMultiplier), false);
            AppendNamedParameter(parameters, "ShipsHealthMultiplier", FormatNullableDouble(settings.ShipsHealthMultiplier), false);
            AppendNamedParameter(parameters, "ShipsDamageMultiplier", FormatNullableDouble(settings.ShipsDamageMultiplier), false);
            AppendNamedParameter(parameters, "BoardingDifficultyMultiplier", FormatNullableDouble(settings.BoardingDifficultyMultiplier), false);
            AppendNamedParameter(parameters, "StatsCorrectionModifier", FormatNullableDouble(settings.StatsCorrectionModifier), false);
            AppendNamedParameter(parameters, "ShipStatsCorrectionModifier", FormatNullableDouble(settings.ShipStatsCorrectionModifier), false);
            AppendNamedParameter(parameters, "CombatDifficulty", NormalizeCombatDifficulty(settings.CombatDifficulty), false);

            if (!string.IsNullOrWhiteSpace(settings.LaunchArguments))
            {
                parameters.Add(settings.LaunchArguments.Trim());
            }

            return string.Join(" ", parameters.ToArray()).Trim();
        }

        private static void AppendNamedParameter(List<string> parameters, string key, string value, bool quoteValue)
        {
            if (parameters == null || value == null)
            {
                return;
            }

            string normalizedValue = quoteValue ? value.Replace("\"", "\\\"") : value.Trim();
            parameters.Add(quoteValue ? $"-{key}=\"{normalizedValue}\"" : $"-{key}={normalizedValue}");
        }

        private static string FormatNullableDouble(double? value)
        {
            return value.HasValue ? value.Value.ToString(CultureInfo.InvariantCulture) : null;
        }

        private static string BuildBaseLaunchArguments(bool embedConsole)
        {
            if (!embedConsole)
            {
                return "-log";
            }

            return "-stdout -FullStdOutLogOutput -AllowStdOutLogVerbosity -forcelogflush -UTF8Output";
        }

        private sealed class BufferedConsoleMirror : IDisposable
        {
            private const int MaxVisibleLines = 5000;
            private const int FlushIntervalMs = 200;

            private readonly int _serverId;
            private readonly SynchronizationContext _consoleContext;
            private readonly List<string> _lines = new List<string>();
            private readonly object _sync = new object();
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private readonly Task _flushTask;
            private bool _dirty;
            private bool _disposed;

            public BufferedConsoleMirror(string serverId, SynchronizationContext consoleContext)
            {
                _serverId = int.TryParse(serverId, out int parsedServerId) ? parsedServerId : -1;
                _consoleContext = consoleContext;
                _flushTask = Task.Run(FlushLoopAsync);
            }

            public void AddOutput(object sender, DataReceivedEventArgs args)
            {
                AddLine(args?.Data);
            }

            public void AddLine(string text)
            {
                if (_disposed || string.IsNullOrWhiteSpace(text))
                {
                    return;
                }

                lock (_sync)
                {
                    _lines.Add(text);

                    int overflow = _lines.Count - MaxVisibleLines;
                    if (overflow > 0)
                    {
                        _lines.RemoveRange(0, overflow);
                    }

                    _dirty = true;
                }
            }

            public void FlushNow()
            {
                if (_serverId < 0)
                {
                    return;
                }

                string snapshot;
                lock (_sync)
                {
                    if (!_dirty)
                    {
                        return;
                    }

                    snapshot = string.Join(Environment.NewLine, _lines.ToArray());
                    _dirty = false;
                }

                void PublishSnapshot()
                {
                    if (MainWindow._serverMetadata.TryGetValue(_serverId, out MainWindow.ServerMetadata metadata) && metadata?.ServerConsole != null)
                    {
                        metadata.ServerConsole.Clear();
                        if (!string.IsNullOrEmpty(snapshot))
                        {
                            metadata.ServerConsole.Add(snapshot);
                        }
                    }
                }

                try
                {
                    if (_consoleContext == null || _consoleContext == SynchronizationContext.Current)
                    {
                        PublishSnapshot();
                    }
                    else
                    {
                        _consoleContext.Post(_ => PublishSnapshot(), null);
                    }
                }
                catch
                {
                }
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                try
                {
                    _cancellationTokenSource.Cancel();
                }
                catch
                {
                }

                FlushNow();
                _cancellationTokenSource.Dispose();
            }

            private async Task FlushLoopAsync()
            {
                try
                {
                    while (!_cancellationTokenSource.IsCancellationRequested)
                    {
                        await Task.Delay(FlushIntervalMs, _cancellationTokenSource.Token);
                        FlushNow();
                    }
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        private static bool WaitForEarlyExit(Process process, int timeoutMs)
        {
            try
            {
                return process.WaitForExit(timeoutMs);
            }
            catch
            {
                return true;
            }
        }

        private static string BuildStartupFailureMessage(Process process, string exePath, DateTime launchTime)
        {
            int exitCode;

            try
            {
                exitCode = process.ExitCode;
            }
            catch
            {
                return $"{Path.GetFileName(exePath)} exited during startup before WindowsGSM could attach to it.";
            }

            string hexCode = $"0x{unchecked((uint)exitCode):X8}";
            string meaning = DescribeExitCode(exitCode);
            string eventDetails = TryGetRecentApplicationEvent(Path.GetFileName(exePath), launchTime);

            var message = new StringBuilder();
            message.Append($"{Path.GetFileName(exePath)} exited during startup with code {exitCode} ({hexCode}).");

            if (!string.IsNullOrWhiteSpace(meaning))
            {
                message.Append(' ');
                message.Append(meaning);
            }

            if (!string.IsNullOrWhiteSpace(eventDetails))
            {
                message.Append(" Windows event: ");
                message.Append(eventDetails);
            }

            return message.ToString();
        }

        private static string DescribeExitCode(int exitCode)
        {
            switch (unchecked((uint)exitCode))
            {
                case 0xC0000135:
                    return "STATUS_DLL_NOT_FOUND: falta una DLL o runtime nativo. Suele resolverse instalando Microsoft Visual C++ Redistributable 2015-2022 x64.";
                case 0xC000007B:
                    return "STATUS_INVALID_IMAGE_FORMAT: hay una dependencia invalida o una mezcla de binarios 32/64 bits.";
                case 0xC0000142:
                    return "STATUS_DLL_INIT_FAILED: una DLL se encontro, pero fallo durante su inicializacion.";
                case 0xC0000005:
                    return "STATUS_ACCESS_VIOLATION: el proceso intento acceder a memoria invalida durante el arranque.";
                default:
                    return string.Empty;
            }
        }

        private static string TryGetRecentApplicationEvent(string executableName, DateTime launchTime)
        {
            try
            {
                using (var applicationLog = new EventLog("Application"))
                {
                    int lowerBound = Math.Max(0, applicationLog.Entries.Count - 20);
                    for (int index = applicationLog.Entries.Count - 1; index >= lowerBound; index--)
                    {
                        EventLogEntry entry = applicationLog.Entries[index];
                        if (entry.TimeGenerated < launchTime.AddSeconds(-2))
                        {
                            break;
                        }

                        string source = entry.Source ?? string.Empty;
                        string message = entry.Message ?? string.Empty;
                        bool relevantSource = source.Equals("Application Error", StringComparison.OrdinalIgnoreCase)
                            || source.Equals("Windows Error Reporting", StringComparison.OrdinalIgnoreCase)
                            || source.Equals("SideBySide", StringComparison.OrdinalIgnoreCase);
                        bool relevantMessage = message.IndexOf(executableName, StringComparison.OrdinalIgnoreCase) >= 0;

                        if (!relevantSource && !relevantMessage)
                        {
                            continue;
                        }

                        string normalizedMessage = Regex.Replace(message, "\\s+", " ").Trim();
                        if (normalizedMessage.Length > 320)
                        {
                            normalizedMessage = normalizedMessage.Substring(0, 320) + "...";
                        }

                        return $"[{source}] {normalizedMessage}";
                    }
                }
            }
            catch
            {
            }

            return string.Empty;
        }

        private static async Task<bool> WaitForExitAsync(Process process, int timeoutMs)
        {
            int elapsed = 0;
            while (elapsed < timeoutMs)
            {
                try
                {
                    if (process.HasExited)
                    {
                        return true;
                    }
                }
                catch
                {
                    return true;
                }

                await Task.Delay(500);
                elapsed += 500;
            }

            try
            {
                return process.HasExited;
            }
            catch
            {
                return true;
            }
        }

        private static bool SendConsoleCommand(Process process, string command)
        {
            if (process == null)
            {
                return false;
            }

            try
            {
                if (process.HasExited)
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }

            if (process.StartInfo.RedirectStandardInput)
            {
                try
                {
                    process.StandardInput.WriteLine(command);
                    return true;
                }
                catch
                {
                }
            }

            if (ConsoleCommandHelper.SendCommand(process.Id, command))
            {
                return true;
            }

            try
            {
                if (process.MainWindowHandle != IntPtr.Zero)
                {
                    ServerConsole.SendMessageToMainWindow(process.MainWindowHandle, command);
                    return true;
                }
            }
            catch
            {
            }

            return false;
        }

        private static async Task HideProcessWindowsAsync(int processId)
        {
            for (int attempt = 0; attempt < 12; attempt++)
            {
                try
                {
                    WindowHelper.HideProcessWindows(processId);
                }
                catch
                {
                }

                await Task.Delay(1000);
            }
        }

        private static WindroseParameterSettings ParsePluginParameters(string rawArguments)
        {
            string launchArguments = rawArguments ?? string.Empty;
            var settings = new WindroseParameterSettings();

            launchArguments = StripCustomArgument(launchArguments, "Password", value => settings.Password = value);
            launchArguments = StripCustomArgument(launchArguments, "IsPasswordProtected", value => settings.IsPasswordProtected = ParseNullableBoolean(value));
            launchArguments = StripCustomArgument(launchArguments, "P2pProxyAddress", value => settings.P2pProxyAddress = value);
            launchArguments = StripCustomArgument(launchArguments, "WorldName", _ => { });
            launchArguments = StripCustomArgument(launchArguments, "WorldPresetType", value => settings.WorldPresetType = value);
            launchArguments = StripCustomArgument(launchArguments, "SharedQuests", value => settings.SharedQuests = ParseNullableBoolean(value));
            launchArguments = StripCustomArgument(launchArguments, "EasyExplore", value => settings.EasyExplore = ParseNullableBoolean(value));
            launchArguments = StripCustomArgument(launchArguments, "MobHealthMultiplier", value => settings.MobHealthMultiplier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "MobDamageMultiplier", value => settings.MobDamageMultiplier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "ShipsHealthMultiplier", value => settings.ShipsHealthMultiplier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "ShipsDamageMultiplier", value => settings.ShipsDamageMultiplier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "BoardingDifficultyMultiplier", value => settings.BoardingDifficultyMultiplier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "StatsCorrectionModifier", value => settings.StatsCorrectionModifier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "ShipStatsCorrectionModifier", value => settings.ShipStatsCorrectionModifier = ParseNullableDouble(value));
            launchArguments = StripCustomArgument(launchArguments, "CombatDifficulty", value => settings.CombatDifficulty = NormalizeCombatDifficulty(value));

            settings.LaunchArguments = Regex.Replace(launchArguments, "\\s{2,}", " ").Trim();
            return settings;
        }

        private static string StripCustomArgument(string arguments, string key, Action<string> onValue)
        {
            if (string.IsNullOrWhiteSpace(arguments))
            {
                return string.Empty;
            }

            string pattern = $"(?i)(?:^|\\s)-{Regex.Escape(key)}=(?:\\\"([^\\\"]*)\\\"|(\\S+))";
            return Regex.Replace(arguments, pattern, match =>
            {
                string value = match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value;
                onValue?.Invoke(value);
                return string.Empty;
            }).Trim();
        }

        private sealed class WindroseParameterSettings
        {
            public string LaunchArguments { get; set; } = string.Empty;
            public string Password { get; set; }
            public bool? IsPasswordProtected { get; set; }
            public string P2pProxyAddress { get; set; }
            public string WorldPresetType { get; set; }
            public bool? SharedQuests { get; set; }
            public bool? EasyExplore { get; set; }
            public double? MobHealthMultiplier { get; set; }
            public double? MobDamageMultiplier { get; set; }
            public double? ShipsHealthMultiplier { get; set; }
            public double? ShipsDamageMultiplier { get; set; }
            public double? BoardingDifficultyMultiplier { get; set; }
            public double? StatsCorrectionModifier { get; set; }
            public double? ShipStatsCorrectionModifier { get; set; }
            public string CombatDifficulty { get; set; }
        }

        private sealed class WorldDescriptionSnapshot
        {
            public string WorldName { get; set; }
            public string WorldPresetType { get; set; }
            public bool? SharedQuests { get; set; }
            public bool? EasyExplore { get; set; }
            public double? MobHealthMultiplier { get; set; }
            public double? MobDamageMultiplier { get; set; }
            public double? ShipsHealthMultiplier { get; set; }
            public double? ShipsDamageMultiplier { get; set; }
            public double? BoardingDifficultyMultiplier { get; set; }
            public double? StatsCorrectionModifier { get; set; }
            public double? ShipStatsCorrectionModifier { get; set; }
            public string CombatDifficulty { get; set; }
        }

        private static class WindowHelper
        {
            [DllImport("user32.dll")]
            private static extern bool EnumWindows(EnumWindowsProc proc, IntPtr lParam);

            [DllImport("user32.dll")]
            private static extern int GetWindowThreadProcessId(IntPtr hWnd, out int processId);

            [DllImport("user32.dll")]
            private static extern bool IsWindowVisible(IntPtr hWnd);

            [DllImport("user32.dll")]
            private static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);

            private delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

            public static void HideProcessWindows(int processId)
            {
                EnumWindows((hWnd, _) =>
                {
                    GetWindowThreadProcessId(hWnd, out int windowProcessId);
                    if (windowProcessId == processId && IsWindowVisible(hWnd))
                    {
                        ShowWindow(hWnd, 0);
                    }

                    return true;
                }, IntPtr.Zero);
            }
        }

        private static class ConsoleCommandHelper
        {
            private const int StdInputHandle = -10;

            [DllImport("kernel32.dll", SetLastError = true)]
            private static extern bool FreeConsole();

            [DllImport("kernel32.dll", SetLastError = true)]
            private static extern bool AttachConsole(int dwProcessId);

            [DllImport("kernel32.dll", SetLastError = true)]
            private static extern IntPtr GetStdHandle(int nStdHandle);

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
            private static extern bool WriteConsoleInput(IntPtr hConsoleInput, InputRecord[] lpBuffer, int nLength, out int written);

            [StructLayout(LayoutKind.Sequential)]
            private struct KeyEventRecord
            {
                public int bKeyDown;
                public short wRepeatCount;
                public short wVirtualKeyCode;
                public short wVirtualScanCode;
                public char UnicodeChar;
                public int dwControlKeyState;
            }

            [StructLayout(LayoutKind.Explicit)]
            private struct InputRecord
            {
                [FieldOffset(0)]
                public short EventType;

                [FieldOffset(4)]
                public KeyEventRecord KeyEvent;
            }

            public static bool SendCommand(int processId, string command)
            {
                FreeConsole();
                if (!AttachConsole(processId))
                {
                    return false;
                }

                try
                {
                    IntPtr inputHandle = GetStdHandle(StdInputHandle);
                    if (inputHandle == IntPtr.Zero || inputHandle == new IntPtr(-1))
                    {
                        return false;
                    }

                    string fullCommand = command + "\r";
                    var records = new InputRecord[fullCommand.Length * 2];
                    int index = 0;

                    foreach (char character in fullCommand)
                    {
                        short virtualKey = character == '\r' ? (short)0x0D : (short)0;
                        records[index++] = new InputRecord
                        {
                            EventType = 1,
                            KeyEvent = new KeyEventRecord
                            {
                                bKeyDown = 1,
                                wRepeatCount = 1,
                                wVirtualKeyCode = virtualKey,
                                UnicodeChar = character
                            }
                        };

                        records[index++] = new InputRecord
                        {
                            EventType = 1,
                            KeyEvent = new KeyEventRecord
                            {
                                bKeyDown = 0,
                                wRepeatCount = 1,
                                wVirtualKeyCode = virtualKey,
                                UnicodeChar = character
                            }
                        };
                    }

                    return WriteConsoleInput(inputHandle, records, records.Length, out int written) && written > 0;
                }
                catch
                {
                    return false;
                }
                finally
                {
                    FreeConsole();
                }
            }
        }
    }
}