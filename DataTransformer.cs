using System.Text.Json;

public class SensorData
{
  public int CompanyId { get; set; } // Foo1: PartnerId, Foo2: CompanyId
  public string CompanyName { get; set; } // Foo1: PartnerName, Foo2: Company
  public int DeviceId { get; set; } // Foo1: Id, Foo2: DeviceID
  public string DeviceName { get; set; } // Foo1: Model, Foo2: Name
  public DateTime? FirstReadingDtm { get; set; } // Foo1: Trackers.Sensors.Crumbs, Foo2: Devices.SensorData
  public DateTime? LastReadingDtm { get; set; }
  public int? TemperatureCount { get; set; }
  public double? AverageTemperature { get; set; }
  public int? HumidityCount { get; set; }
  public double? AverageHumidity { get; set; }
}

public class SensorRawData
{
  public int CompanyId { get; set; }
  public string CompanyName { get; set; }
  public int DeviceId { get; set; }
  public string DeviceName { get; set; }
  public bool isTemperature { get; set; }
  public bool isHumidity { get; set; }
  public DateTime CreatedDtm { get; set; }
  public double Value { get; set; }
}

public static class DeviceDataProcessor
{
  static public List<SensorRawData> SensorDataList = new List<SensorRawData>();

  static JsonElement ReadJson(string jsonFilePath)
  {
    var json = File.ReadAllText("data/" + jsonFilePath);
    return JsonDocument.Parse(json).RootElement;
  }

  public static void Process()
  {
    JsonElement commands = ReadJson("config.json");
    foreach (JsonElement item in commands.EnumerateArray())
    {
      var operation = item.GetProperty("operation").GetString();
      if (operation != "merge")
      {
        throw new Exception("Operation not supported.");
      }

      SensorDataList = new List<SensorRawData>();
      var sources = item.GetProperty("sources");
      var destination = item.GetProperty("destination").GetString();
      var transformations = item.GetProperty("transformations");
      Dictionary<string, JsonElement> mappingsMap = new Dictionary<string, JsonElement>();
      foreach (JsonElement transformation in transformations.EnumerateArray())
      {
        var id = transformation.GetProperty("id").GetString();
        var mapping = transformation.GetProperty("mapping");
        mappingsMap[id] = mapping;
      };

      foreach (JsonElement source in sources.EnumerateArray())
      {
        var file = source.GetProperty("file").GetString();
        var transformationId = source.GetProperty("transformation").GetString();

        var mapping = mappingsMap[transformationId];
        var sourceObject = ReadJson(file);

        var dictionary = new Dictionary<string, object>();
        IterateJsonElement(mapping, sourceObject, dictionary);
      };

      // Aggregate SensorRawData to SensorData
      var result = TransformData(SensorDataList);
      SaveDataToJsonFile(result, destination);
    };
  }

  static void IterateJsonElement(JsonElement mapping, JsonElement source, Dictionary<string, object> dictionary)
  {
    foreach (JsonProperty property in mapping.EnumerateObject())
    {
      if (property.Name.StartsWith("is"))
      {
        var equation = property.Value.ToString();
        var parts = equation.Split(new[] { "==" }, StringSplitOptions.None);
        if (parts.Length != 2)
        {
          throw new Exception("The equation does not contain exactly one '=='.");
        }

        var (a, b) = (parts[0].Trim(), parts[1].Trim());
        dictionary[property.Name] = source.GetProperty(a).ToString() == b;
        continue;
      }

      switch (property.Value.ValueKind)
      {
        case JsonValueKind.String:
          dictionary[property.Value.ToString()] = source.GetProperty(property.Name);
          if (property.Value.ToString() == "Value")
          {
            SensorDataList.Add(new SensorRawData
            {
              CompanyId = int.Parse(dictionary["CompanyId"].ToString()),
              CompanyName = dictionary["CompanyName"].ToString(),
              DeviceId = int.Parse(dictionary["DeviceId"].ToString()),
              DeviceName = dictionary["DeviceName"].ToString(),
              isTemperature = bool.Parse(dictionary["isTemperature"].ToString()),
              isHumidity = bool.Parse(dictionary["isHumidity"].ToString()),
              CreatedDtm = DateTime.Parse(dictionary["Dtm"].ToString()),
              Value = double.Parse(dictionary["Value"].ToString())
            });
          }
          break;
        case JsonValueKind.Object:
          foreach (JsonElement sourceItem in source.GetProperty(property.Name).EnumerateArray())
          {
            IterateJsonElement(property.Value, sourceItem, dictionary);
          }
          break;
      };
    };
  }

  public static List<SensorData> TransformData(List<SensorRawData> rawData)
  {
    var sensorDataList = rawData
        .GroupBy(data => new { data.CompanyId, data.DeviceId })
        .Select(group => new SensorData
        {
          CompanyId = group.Key.CompanyId,
          CompanyName = group.First().CompanyName,
          DeviceId = group.Key.DeviceId,
          DeviceName = group.First().DeviceName,
          FirstReadingDtm = group.Min(data => data.CreatedDtm),
          LastReadingDtm = group.Max(data => data.CreatedDtm),
          TemperatureCount = group.Count(data => data.isTemperature),
          AverageTemperature = group.Where(data => data.isTemperature).Average(data => (double?)data.Value),
          HumidityCount = group.Count(data => data.isHumidity),
          AverageHumidity = group.Where(data => data.isHumidity).Average(data => (double?)data.Value)
        })
        .ToList();

    return sensorDataList;
  }

  public static void SaveDataToJsonFile(List<SensorData> sensorDataList, string filePath)
  {
    var options = new JsonSerializerOptions
    {
      WriteIndented = true
    };
    string jsonString = JsonSerializer.Serialize(sensorDataList, options);
    File.WriteAllText(filePath, jsonString);
  }
}
