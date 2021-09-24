using Microsoft.Azure.Devices.Client;
using System.Text;
using System.Text.Json;

var sourceFolder = "/mnt/data";
var movedFolder = Path.Combine(sourceFolder, "done");
if (!Directory.Exists(movedFolder)) Directory.CreateDirectory(movedFolder);

var deviceClient = DeviceClient.CreateFromConnectionString("");

while (true)
{
    foreach (var filename in Directory.EnumerateFiles(movedFolder, "*.csv"))
    {
        await Task.Delay(1000);
        Console.WriteLine($"Handling {filename}");
        var content = File.ReadAllText(filename);
        foreach (var line in content.Split())
        {
            var parts = line.Split(',');
            var timestamp = DateTime.Parse(parts[0]);
            var value = parts[1];

            var payload = new
            {
                timestamp,
                value
            };
            var json = JsonSerializer.Serialize(payload);
            var bytes = Encoding.UTF8.GetBytes(json);
            var message = new Message(bytes);

            await deviceClient.SendEventAsync(message);
        }
        File.Move(filename, movedFolder);
    }
    await Task.Delay(1000);
}