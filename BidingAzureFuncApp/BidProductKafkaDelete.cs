using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace BidingAzureFuncApp
{
    public class BidProductKafkaDelete
    {
        // KafkaTrigger sample 
        // Consume the message from "topic" on the LocalBroker.
        // Add `BrokerList` and `KafkaPassword` to the local.settings.json
        // For EventHubs
        // "BrokerList": "{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"
        // "KafkaPassword":"{EVENT_HUBS_CONNECTION_STRING}
        [FunctionName("BidProductKafkaDelete")]
        public static void Run(
            [KafkaTrigger("host.docker.internal:9092",
                          "DeleteProduct",
                          //Username = "$ConnectionString",
                          //Password = "%KafkaPassword%",
                          Protocol = BrokerProtocol.Plaintext,
                          AuthenticationMode = BrokerAuthenticationMode.Plain,
                          ConsumerGroup = "$Default")] KafkaEventData<string>[] events, ILogger log)
        {
            string deleteValue = string.Empty;
            char s = '"';
            string t = string.Empty;
            foreach (KafkaEventData<string> eventData in events)
            {
                deleteValue = eventData.Value.Replace(s.ToString(), t);
                log.LogInformation($"C# Kafka trigger function processed a message: {eventData.Value}");
            }
            try
            {
                DataTable dtdb = new DataTable();
                //you can get connection string as follows
                string connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
                using (SqlConnection cons = new SqlConnection(connectionString))
                {
                    log.LogInformation("DB Execution Started");

                    cons.Open();
                    SqlCommand cmds = new SqlCommand();
                    cmds.Connection = cons;
                    cmds.CommandText = "[dbo].[USP_DeleteProduct]";
                    cmds.CommandType = CommandType.StoredProcedure;
                    //params
                    cmds.Parameters.Add("@productId", SqlDbType.VarChar).Value = deleteValue.ToString();

                    //
                    using (SqlDataAdapter adp = new SqlDataAdapter(cmds))
                    {
                        adp.Fill(dtdb);
                    }
                    cons.Close();
                    log.LogInformation("DB Executed");
                }
                var result = (from DataRow m in dtdb.AsEnumerable() select m).ToList();
                log.LogInformation(result.ElementAtOrDefault(0).Field<string>("MSG"));
                //log.LogInformation($"C# Kafka trigger function processed the product deletion");

            }
            catch(Exception ex)
            {
                log.LogError(ex.Message);
            }
        }
    }
}
