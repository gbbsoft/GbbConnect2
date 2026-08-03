using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Xml;
using GbbConnect2Protocol;
using GbbEngine2.Configuration;
using GbbEngine2.Drivers;
using GbbEngine2.Drivers.Random;
using GbbEngine2.Drivers.SolarmanV5;
using GbbLibSmall;
using MQTTnet;

namespace GbbEngine2.Server
{
    public partial class JobManager
    {
        private static MqttClientFactory mqttFactory = new();
        DateTime? LastClearOldLogs = null;

        private async void OurMqttService(Configuration.Parameters Parameters, CancellationToken ct, IOurLog log)
        {

            try
            {
#if DEBUG
                // wait for server to start
                await Task.Delay(5 * 1000, ct);
#endif

                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        // start
                        //log.OurLog(LogLevel.Information, "MqttService: starting");
                        await OurMqttService_DoWork(Parameters, ct, log);

                    }
                    catch (TaskCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex) // eg System.Threading.Tasks.TaskCanceledException
                    {
                        // log
                        log.OurLog(LogLevel.Error, $"MqttServices: {ex}");
                    }

                    // try again after delay
                    if (!ct.IsCancellationRequested)
                    {
#if DEBUG
                        log.OurLog(LogLevel.Error, $"MqttServices: waiting 10 sec");
                        await Task.Delay(10 * 1000, ct);
#else
                        log.OurLog(LogLevel.Error, $"MqttServices: waiting 5min");
                        await Task.Delay(5*60 * 1000, ct);
#endif
                    }
                }
            }
            catch(TaskCanceledException)
            {
                throw;
            }
            // log
            log.OurLog(LogLevel.Information, "MqttService: finished");
        }


        private async Task ConnectToMqtt(Parameters Parameters, Plant plant, IMqttClient client, CancellationToken ct, IOurLog log)
        {
            if (string.IsNullOrWhiteSpace(plant.GbbOptimizer_PlantId))
                throw new ApplicationException("GbbConnect2: No PlantId!");
            if (string.IsNullOrWhiteSpace(plant.GbbOptimizer_Mqtt_Address))
                throw new ApplicationException("GbbConnect2: No Mqtt address!");

            var b = new MqttClientOptionsBuilder()
                .WithClientId($"GbbConnect2_{plant.GbbOptimizer_PlantId?.ToString()}")
                //.WithCleanSession(true)
                .WithTlsOptions(new MqttClientTlsOptions()
                {
#if DEBUG
                    //UseTls = true,
                    // 2023-12-15: nie ma juz potrzeby
                    IgnoreCertificateChainErrors = true,
#else
                    UseTls = true,
               
#endif
                })
                .WithTcpServer(plant.GbbOptimizer_Mqtt_Address, plant.GbbOptimizer_Mqtt_Port)
                .WithCredentials(plant.GbbOptimizer_PlantId?.ToString(), plant.GbbOptimizer_PlantToken);
            //.WithSessionExpiryInterval(2*60); // nie wiadomo w jakich to jest jednostkach!

            // connect
            await client.ConnectAsync(b.Build(), ct);


            // subsribe
            await client.SubscribeAsync(
                mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(q =>
                        q.WithTopic($"{plant.GbbOptimizer_PlantId?.ToString()}/ModbusInMqtt/toDevice")
                        .WithAtLeastOnceQoS()
                        )
                .Build()
                , ct);

        }

        private async Task OurMqttService_DoWork(Configuration.Parameters Parameters, CancellationToken ct, IOurLog log)
        {

            int Counter = 0;

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    DateTime LoopStartTime = DateTime.Now;

                    // =====================================
                    // Clear Old Logs
                    // =====================================
                    if (LastClearOldLogs == null || LastClearOldLogs.Value.Date != DateTime.Now.Date)
                    {
                        LastClearOldLogs = DateTime.Now;
                        try
                        {
                            Parameters.DoClearOldLogs(log);
                        }
                        catch (TaskCanceledException)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            log.OurLog(LogLevel.Error, $"ClearOldLogs: {ex.Message}");
                        }
                    }


                    // =====================================
                    // Connect / Reconnect
                    // =====================================
                    foreach (var plant in Parameters.Plants)
                    {
                        try
                        {
                            if (plant.PlantState!=null
                                && plant.IsDisabled == 0
                                && (plant.PlantState.MqttClient==null || !plant.PlantState.MqttClient.IsConnected)
                                && plant.GbbOptimizer_PlantId != null 
                                && plant.GbbOptimizer_PlantToken != null)
                            {
                                log.OurLog(LogLevel.Information, $"{plant.Name}: Starting Mqtt");
                                var client = mqttFactory.CreateMqttClient();

                                // callback
                                client.ApplicationMessageReceivedAsync += e => { return MqttClient_MessageReceivedAsync(Parameters, e, plant, log); };

                                // Use builder classes where possible in this project.
                                await ConnectToMqtt(Parameters, plant, client, ct, log);

                                // save client
                                plant.PlantState!.MqttClient = client;

                                log.OurLog(LogLevel.Information, $"{plant.Name}: Started Mqtt");
                                Counter++;
                            }
                        }
                        catch (TaskCanceledException)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            log.OurLog(LogLevel.Error, $"{plant.Name}: {ex.Message}");
                        }

                        ct.ThrowIfCancellationRequested();
                    }
                    // nothing to do
                    if (Counter == 0)
                        break;


                    // ===============
                    // keep alive
                    // ===============
                    foreach (var plant in Parameters.Plants)
                    {
                        ct.ThrowIfCancellationRequested();

                        try
                        {
                            if (plant.PlantState!.MqttClient!= null && plant.PlantState!.MqttClient.IsConnected)
                            {

                                if (Parameters.IsVerboseLog)
                                {
                                    log.OurLog(LogLevel.Information, $"{plant.Name}: Mqtt: Sending keepalive");
                                }


                                await plant.PlantState!.MqttClient.PublishAsync(
                                    new MqttApplicationMessageBuilder()
                                    .WithTopic($"{plant.GbbOptimizer_PlantId?.ToString()}/keepalive")
                                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                                    .Build()
                                    , ct);
                            }
                        }
                        catch (TaskCanceledException)
                        {
                            throw;
                        }
                        catch (Exception ex) 
                        {
                            log.OurLog(LogLevel.Error, $"{plant.Name}: Mqtt: {ex.Message}");
                        }
                    }


                    // ======================================
                    // check if no invSetup from GbbOptimizer
                    // ======================================
                    if (LoopStartTime.Minute>10)
                    {
                        foreach (var plant in Parameters.Plants)
                        {
                            ct.ThrowIfCancellationRequested();
                            try
                            {
                                // check if any InvSetup from GbbOptimizer in this hour
                                if (plant.LastIsInvSetup != null 
                                && plant.LastIsInvSetup.Value < new DateTime(LoopStartTime.Year, LoopStartTime.Month, LoopStartTime.Day, LoopStartTime.Hour, 0,0))
                                {
                                    log.OurLog(LogLevel.Information, $"{plant.Name}: No InvSetup from GbbOptimizer in this hour, sending NoConn lines");
                                    await OurSendNoConnLines(Parameters, plant, log);

                                    // do it only once
                                    plant.LastIsInvSetup = null;
                                }
                            }
                            catch (TaskCanceledException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                log.OurLog(LogLevel.Error, $"{plant.Name}: {ex.Message}");
                            }
                        }
                    }


                    // =====================================
                    // try keep 1min between keep-alive
                    // =====================================
                    var ms = (int)(LoopStartTime.AddMinutes(1) - DateTime.Now).TotalMilliseconds;
                    if (ms > 0)
                        await Task.Delay(ms, ct);
                }
            }
            finally
            {
                foreach(var plant in Parameters.Plants)
                {
                    if (plant.PlantState!.MqttClient != null)
                    {
                        try
                        {
                            // disconnect
                            log.OurLog(LogLevel.Information, $"{plant.Name}: Disconnecting Mqtt");
                            var mqttClientDisconnectOptions = mqttFactory.CreateClientDisconnectOptionsBuilder().Build();
                            await plant.PlantState!.MqttClient.DisconnectAsync(mqttClientDisconnectOptions, CancellationToken.None);
                        }
                        catch (Exception ex)
                        {
                            log.OurLog(LogLevel.Error, $"{plant.Name}: {ex.Message}");
                        }
                    }
                }

            }

        }


        // ======================================


        // ======================================


        private async Task MqttClient_MessageReceivedAsync(Configuration.Parameters Parameters, MqttApplicationMessageReceivedEventArgs arg, Configuration.Plant Plant, IOurLog log)
        {
            string? Operation = null;

            // options for json serialization
            var SerOpt = new JsonSerializerOptions();
            SerOpt.DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull;


            try
            {
                var seg = arg.ApplicationMessage.ConvertPayloadToString();
                if (Parameters.IsVerboseLog)
                {
                    log.OurLog(LogLevel.Information, $"{Plant.Name}: Mqtt: Received request: {seg}");
                }


                var jsonOptions = new JsonSerializerOptions();
                jsonOptions.AllowTrailingCommas = true;
                jsonOptions.DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull;

                var Header = JsonSerializer.Deserialize<Header>(seg, jsonOptions);
                if (Header != null)
                {
                    Header.ProtocolVersion = Header.CURR_PROTOCOL_VERSION;
                    Header.ClientVersion = Parameters.APP_VERSION;
                    Header.ClientEnvironment = Parameters.APP_ENVIRONMENT;
                    Header.ClientName = Parameters.APP_NAME;
                    Header.ClientInfo = $"Inverter IP: {Plant.AddressIP}, Program IP: {Parameters.MyIpAddress}";

                    // ==========================
                    // Change log level
                    // ==========================
                    if (Header.LogLevel != null)
                    {
                        log.ChangeParameterProperty(new Action(() =>
                        {
                            if (string.Compare(Header.LogLevel, Header.LogLevel_ONLY_ERRORS, true) == 0)
                            {
                                Parameters.IsVerboseLog = false;
                                Parameters.IsDriverLog = false;
                                Parameters.IsDriverLog2 = false;
                            }
                            else if (string.Compare(Header.LogLevel, Header.LogLevel_MIN, true) == 0)
                            {
                                Parameters.IsVerboseLog = true;
                                Parameters.IsDriverLog = false;
                                Parameters.IsDriverLog2 = false;
                            }
                            else if (string.Compare(Header.LogLevel, Header.LogLevel_MAX, true) == 0)
                            {
                                Parameters.IsVerboseLog = true;
                                Parameters.IsDriverLog = true;
                                Parameters.IsDriverLog2 = true;
                            }
                            else
                                log.OurLog(LogLevel.Warning, $"{Plant.Name}: Mqtt: Unknown log level: {Header.LogLevel}");
                            Parameters.Save();
                        }));
                    }

                    // ==========================
                    // find parameters
                    // ==========================
                    string? addressIP = Plant.AddressIP;
                    int? portNo = Plant.PortNo;
                    long? dongielSerialNo = Plant.SerialNumber;

                    // send to subInverter?
                    if (!string.IsNullOrWhiteSpace(Header.SubInverterSN))
                    {
                        bool OK = false;
                        foreach (var inv in Plant.SubInverters)
                        {
                            if (inv.SerialNumber.ToString() == Header.SubInverterSN.Trim())
                            {
                                addressIP = inv.AddressIP;
                                portNo = inv.PortNo;
                                dongielSerialNo = inv.DongleSerialNumber;

                                // save last will
                                if (Header.LinesOnNoInvSetup != null)
                                {
                                    inv.LinesOnNoInvSetup = Header.LinesOnNoInvSetup;
                                    if (Parameters.IsDriverLog)
                                        log.OurLog(LogLevel.Information, $"Receiving LinesOnNoInvSetup for dongle SN={inv.DongleSerialNumber}");
                                }

                                OK = true;
                                break;
                            }
                        }
                        if (!OK)
                            throw new ApplicationException($"Inverter SerialNumber not found: {Header.SubInverterSN} on Slave Inverters list!");
                    }
                    else if (Header.LinesOnNoInvSetup != null)
                    {
                        // save last will
                        Plant.LinesOnNoInvSetup = Header.LinesOnNoInvSetup;
                        if (Parameters.IsDriverLog)
                            log.OurLog(LogLevel.Information, $"Receiving LinesOnNoInvSetup for master dongle");
                    }


                    if (Header.IsInvSetup!=0)
                        Plant.LastIsInvSetup = DateTime.Now;


                    // ==========================
                    // process lines
                    // ==========================
                    await SendLines(Parameters, Plant, log, Header, addressIP, portNo, dongielSerialNo);

                    // ==========================
                    // To log without LastLog
                    // ==========================

                    if (Parameters.IsVerboseLog)
                    {
                        var payload0 = JsonSerializer.Serialize(Header, jsonOptions);
                        log.OurLog(LogLevel.Information, $"{Plant.Name}: Mqtt: Send response: {payload0}");
                    }


                    // ==========================
                    // Add Last log
                    // ==========================
                    var NewLastLog_Date = Plant.PlantState?.LastLog_Date;
                    var NewLastLog_Pos = Plant.PlantState?.LastLog_Pos;


                    try
                    {
                        if (Header.SendLastLog != 0)
                        {
                            DateTime td = DateTime.Today;
                            string DirName = Path.Combine(Parameters.OurGetUserBaseDirectory(), "Log");

                            if (NewLastLog_Date == null || NewLastLog_Date < td.AddDays(-1)
                                || NewLastLog_Pos == null)
                            {
                                NewLastLog_Date = td;
                                NewLastLog_Pos = 0;

                                string FileName = Path.Combine(DirName, $"{td:yyyy-MM-dd}.txt");
                                if (File.Exists(FileName))
                                    NewLastLog_Pos = new FileInfo(FileName).Length;
                            }
                            else
                            {
                                string FileName = Path.Combine(DirName, $"{NewLastLog_Date:yyyy-MM-dd}.txt");
                                if (File.Exists(FileName))
                                {
                                    // read file
                                    using (var fs = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                                    {
                                        fs.Seek(NewLastLog_Pos.Value, SeekOrigin.Begin);
                                        using (var sr = new StreamReader(fs))
                                        {
                                            string? s = sr.ReadToEnd();
                                            if (s != null)
                                            {
                                                Header.LastLog = s;
                                                NewLastLog_Pos = fs.Position;
                                            }
                                        }
                                    }
                                }

                                // new day, next time send log from current day
                                if (NewLastLog_Date == td.AddDays(-1))
                                {
                                    NewLastLog_Date = td;
                                    NewLastLog_Pos = 0;
                                }
                            }
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        log.OurLog(LogLevel.Error, $"ERRO: Get Last Log: {ex.Message}");
                    }

                    // ==========================
                    // Send responce
                    // ==========================
                    var payload = JsonSerializer.Serialize(Header, jsonOptions);
                    await Plant.PlantState!.MqttClient!.PublishAsync(
                        new MqttApplicationMessageBuilder()
                       .WithTopic($"{Plant.GbbOptimizer_PlantId?.ToString()}/ModbusInMqtt/fromDevice")
                       .WithPayload(payload)
                       .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                       .Build()
                       , CancellationToken.None);


                    Plant.PlantState.LastLog_Date = NewLastLog_Date;
                    Plant.PlantState.LastLog_Pos = NewLastLog_Pos;
                    Plant.PlantState.OurSaveState();

                }

            }
            catch (Exception ex)
            {
                log.OurLog(LogLevel.Error, $"{Plant.Name}: {ex.Message}");

                if (Operation != null && Plant.PlantState!.MqttClient != null)
                {
                    try
                    {
                        // send error resonse
                        //var Res = new Response();
                        //Res.Operation = Operation;
                        //Res.Status = "ERROR";
                        //Res.ErrDesc = ex.Message;

                        //await Plant.PlantState!.MqttClient.PublishAsync(
                        //    new MqttApplicationMessageBuilder()
                        //   .WithTopic($"{Plant.GbbOptimizer_PlantId.ToString()}/dataresponse")
                        //   .WithPayload(JsonSerializer.Serialize(Res, SerOpt))
                        //   .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        //   .Build()
                        //   , CancellationToken.None);

                    }
                    catch (Exception ex2)
                    {
                        log.OurLog(LogLevel.Error, $"{Plant.Name}: {ex2.Message}");
                    }

                }
                
            }


        }


        /// <summary>
        /// send last will data to inverter if connection with GbbOptimizer is lost
        /// </summary>
        /// <param name="Parameters"></param>
        /// <param name="Plant"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        public static async Task OurSendNoConnLines(Parameters Parameters, Plant Plant, IOurLog log)
        {
            if (Plant.LinesOnNoInvSetup != null)
            {
                log.OurLog(LogLevel.Information, $"{Plant.Name}: Sending NoConn lines to master dongle");

                Header Header = new Header();
                Header.Lines = Plant.LinesOnNoInvSetup;
                await SendLines(Parameters, Plant, log, Header, Plant.AddressIP, Plant.PortNo, Plant.SerialNumber);

                // log errors
                if (Header.Error!=null)
                    log.OurLog(LogLevel.Error, $"{Plant.Name}: {Header.Error}");
                if (Header.Lines!=null)
                {
                    for (int i = 0; i < Header.Lines.Length; i++)
                    {
                        var line = Header.Lines[i];
                        if (line != null && line.Error != null)
                            log.OurLog(LogLevel.Error, $"{Plant.Name}: LineNo={line.LineNo}: {line.Error}");
                    }
                }
                // send only once
                Plant.LinesOnNoInvSetup = null;
            }
            foreach (var inv in Plant.SubInverters)
            {
                log.OurLog(LogLevel.Information, $"{Plant.Name}: Sending NoConn lines to dongle SN={inv.DongleSerialNumber}");

                Header Header = new Header();
                Header.Lines = inv.LinesOnNoInvSetup;
                await SendLines(Parameters, Plant, log, Header, inv.AddressIP, inv.PortNo, inv.DongleSerialNumber);

                // log errors
                if (Header.Error != null)
                    log.OurLog(LogLevel.Error, $"{Plant.Name}: SN={inv.DongleSerialNumber}: {Header.Error}");
                if (Header.Lines != null)
                {
                    for (int i = 0; i < Header.Lines.Length; i++)
                    {
                        var line = Header.Lines[i];
                        if (line != null && line.Error != null)
                            log.OurLog(LogLevel.Error, $"{Plant.Name}: SN={inv.DongleSerialNumber}: LineNo={line.LineNo}: {line.Error}");
                    }
                }
                // send only once
                inv.LinesOnNoInvSetup = null;
            }

        }

        private static async Task SendLines(Parameters Parameters, Plant Plant, IOurLog log, Header Header, string? addressIP, int? portNo, long? dongielSerialNo)
        {
            // ==========================
            // process lines
            // ==========================
            IDriver? drv = null;
            try
            {


                // get driver
                switch (Plant.DriverNo)
                {
                    case (int)GbbEngine2.Drivers.DriverInfo.Drivers.i000_SolarmanV5:
                        {
                            SolarmanV5Driver sm = new SolarmanV5Driver(Parameters, addressIP, portNo, dongielSerialNo, log);
                            sm.Connect();
                            drv = sm;
                        }
                        break;

                    case (int)GbbEngine2.Drivers.DriverInfo.Drivers.i001_ModbusTCP:
                        {
                            ModbusTcpDriver sm = new ModbusTcpDriver(Parameters, addressIP, portNo, dongielSerialNo, log);
                            sm.Connect();
                            drv = sm;
                        }
                        break;

                    case (int)GbbEngine2.Drivers.DriverInfo.Drivers.i999_Random:
                        drv = new RandomDriver();
                        break;

                    default:
                        throw new ApplicationException("Unknown driver no: " + Plant.DriverNo);
                }

                if (Header.Lines != null)
                {
                    for (int i = 0; i < Header.Lines.Length; i++)
                    {
                        var line = Header.Lines[i];
                        if (line != null)
                        {
                            try
                            {
                                if (line.Modbus != null)
                                {
                                    // send to device
                                    line.Modbus = GbbLibSmall2.Convert.ButesToString(await drv.SendDataToDevice(GbbLibSmall2.Convert.StringToBytes(line.Modbus)));
                                }
                            }
                            catch (Exception ex)
                            {
                                line.Error = ex.Message;
                                // clear rest  Modbus data
                                for (; i < Header.Lines.Length; i++)
                                    Header.Lines[i].Modbus = null;
                                break;
                            }
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                Header.Error = ex.Message;
                if (Header.Lines != null)
                    for (int i = 0; i < Header.Lines.Length; i++)
                        Header.Lines[i].Modbus = null;
            }
            finally
            {
                if (drv != null)
                {
                    drv.Dispose();
                    drv = null;
                }
            }
        }
    }
}
